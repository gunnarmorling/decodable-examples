package dev.morling.demos.txbuffering;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat;
import org.apache.flink.streaming.api.functions.sink.PrintSink;
import org.apache.kafka.common.Uuid;

import com.fasterxml.jackson.databind.ObjectMapper;

public class DataStreamV2Job {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getInstance();
//		((ExecutionEnvironmentImpl)env).getConfiguration().set(PipelineOptions.GENERIC_TYPES, false);
		String bootstrap_servers = "localhost:9092";

		DebeziumJsonDecodingFormat decodingFormat = new DebeziumJsonDecodingFormat(true, false, TimestampFormat.ISO_8601);
//		decodingFormat.applyReadableMetadata(List.of("source.properties"));
//
//		DeserializationSchema<RowData> customerDecoder = decodingFormat.createRuntimeDecoder(
//				ScanRuntimeProviderContext.INSTANCE,
//				DataTypes.ROW(
//				        DataTypes.FIELD("id", DataTypes.BIGINT()),
//				        DataTypes.FIELD("first_name", DataTypes.STRING()),
//				        DataTypes.FIELD("last_name", DataTypes.STRING()),
//				        DataTypes.FIELD("email", DataTypes.STRING())
//				    )
//		);

		JsonDeserializationSchema<DataChangeEvent> jsonFormat=new JsonDeserializationSchema<>(DataChangeEvent.class);


		KafkaSource<DataChangeEvent> customersSource = KafkaSource.<DataChangeEvent>builder()
		        .setBootstrapServers(bootstrap_servers)
//		        .setTopics("demodb.transaction")
		        .setTopics("dbserver1.inventory.customers")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
//		        .setValueOnlyDeserializer(new SimpleStringSchema())
		        .setValueOnlyDeserializer(jsonFormat)
		        .setProperty("acks", "all")
		        .build();

//		DeserializationSchema<RowData> orderDecoder = decodingFormat.createRuntimeDecoder(
//				ScanRuntimeProviderContext.INSTANCE,
//				DataTypes.ROW(
//				        DataTypes.FIELD("id", DataTypes.BIGINT()),
//				        DataTypes.FIELD("purchaser", DataTypes.BIGINT()),
//				        DataTypes.FIELD("quantity", DataTypes.STRING())
//				    )
//		);


		KafkaSource<DataChangeEvent> ordersSource = KafkaSource.<DataChangeEvent>builder()
		        .setBootstrapServers(bootstrap_servers)
//		        .setTopics("demodb.transaction")
		        .setTopics("dbserver1.inventory.orders")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
//		        .setValueOnlyDeserializer(new SimpleStringSchema())
		        .setValueOnlyDeserializer(jsonFormat)
		        .setProperty("acks", "all")
		        .build();



//		NonKeyedPartitionStream<String> input =
//				env.fromSource(
//						DataStreamV2SourceUtils.fromData(Arrays.asList("1", "2", "3")),
//						"source"
//						);

		KeyedPartitionStream<Long, DataChangeEvent> customers =
				env.fromSource(
						DataStreamV2SourceUtils.wrapSource(customersSource),
						"customers-source"
						)
				.keyBy(r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("id")) : Long.valueOf((int)r.after().get("id")))
				.process(new WatermarkAssignmentFunction(), r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("id")) : Long.valueOf((int)r.after().get("id")));

		KeyedPartitionStream<Long, DataChangeEvent> orders =
				env.fromSource(
						DataStreamV2SourceUtils.wrapSource(ordersSource),
						"orders-source"
						)
				.keyBy(r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("purchaser")) : Long.valueOf((int)r.after().get("purchaser")))
				.process(new WatermarkAssignmentFunction(), r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("purchaser")) : Long.valueOf((int)r.after().get("purchaser")));

//		KeyedPartitionStream<Long,RowData> customers =

		customers.connectAndProcess(orders, new MyJoinFunction(new Joiner(), JoinType.INNER))

//		input.

//		BuiltinFuncs.join(
//				  customers,
//				  orders,
//				  new Joiner()
//				)
		.keyBy(r -> Long.valueOf((int)r.left().after().get("id")))
		.process(new MaterializationFunction())
		.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>()));


//		NonKeyedPartitionStream<RowData> parsed = input.process(
//				new OneInputStreamProcessFunction<RowData, RowData>() {
//
//					@Override
//					public void processRecord(RowData record, Collector<RowData> output, PartitionedContext<RowData> ctx)
//							throws Exception {
//
//						output.collect(record);
//					}
//
//				});

//		customers.process(new CustomProcessFunction())
//		.process(new OneInputStreamProcessFunction<RowData, RowData>() {
//
//			@Override
//			public void processRecord(RowData record, Collector<RowData> output, PartitionedContext<RowData> ctx)
//					throws Exception {
//				System.out.println("#### PROCESSING: " + record);
//
//				output.collect(record);
//			}
//
//		})
//		.process(new CustomProcessFunction1())
//
//		.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>()));

		env.execute("my job");
	}

	public static class MaterializationFunction implements OneInputStreamProcessFunction<DataChangeEventPair, String> {

//		private static final ValueStateDeclaration<String> CUSTOMER_STATE = StateDeclarations.valueState("customer",s TypeDescriptors.STRING);
		private static final MapStateDeclaration<Long, String> CUSTOMER_STATE = StateDeclarations.mapState("customers", TypeDescriptors.LONG, TypeDescriptors.STRING);

		private transient ObjectMapper objectMapper;

		@Override
		public void open(NonPartitionedContext<String> ctx) throws Exception {
			objectMapper = new ObjectMapper();
		}

		@Override
		public Set<StateDeclaration> usesStates() {
			return Set.of(CUSTOMER_STATE);
		}


		@Override
		public void processRecord(DataChangeEventPair record, Collector<String> output, PartitionedContext<String> ctx) throws Exception {
            MapState<Long, String> state = ctx.getStateManager().getState(CUSTOMER_STATE);

            for(Entry<Long, String> entry : state.entries()) {
            	if (entry.getKey() >= record.txId()) {
            		CustomerWithOrders customer = objectMapper.readValue(entry.getValue(), CustomerWithOrders.class);
                	customer = customer.updateFromDataChangeEventPair(record);
                	entry.setValue(objectMapper.writeValueAsString(customer));
            	}
            }

            System.out.println("### AGGR - Storing " + record.txId());
            String currentTxState = state.get(record.txId());

            if (currentTxState == null) {
            	Optional<String> latestPriorState = getLatestPriorState(record.txId(), state);

            	if(latestPriorState.isPresent()) {
            		CustomerWithOrders customer = objectMapper.readValue(latestPriorState.get(), CustomerWithOrders.class);
                	customer = customer.updateFromDataChangeEventPair(record);
                	state.put(record.txId(), objectMapper.writeValueAsString(customer));
            	}
            	else {
            		CustomerWithOrders customer = CustomerWithOrders.fromDataChangeEventPair(record);
            		state.put(record.txId(), objectMapper.writeValueAsString(customer));
            	}
            }


//            CustomerWithOrders customer = null;
//            if (state.value() == null) {
//            	customer = CustomerWithOrders.fromDataChangeEventPair(record);
//            }
//            else {
//            	customer = objectMapper.readValue(state.value(), CustomerWithOrders.class);
//            	customer = customer.updateFromDataChangeEventPair(record);
//            }
//
//            if (customer != null) {
//            	state.update(objectMapper.writeValueAsString(customer));
//            	output.collect(objectMapper.writeValueAsString(customer));
//            }
//            else {
//            	state.clear();
//            	output.collect(objectMapper.writeValueAsString(customer));
//            }

//            System.out.println("### VALUE: " + state.value());

//             System.out.println("Receiving record: " + record.left().source().get("txId") + ", " + record.right().source().get("txId"));
		}

		private Optional<String> getLatestPriorState(long txId, MapState<Long, String> state) {
			long latestPriorTxId = Long.MIN_VALUE;
			String latestPriorState = null;

			for(Entry<Long, String> entry : state.entries()) {
				if (entry.getKey() > latestPriorTxId && entry.getKey() < txId) {
					latestPriorTxId = entry.getKey();
					latestPriorState = entry.getValue();
				}
			}

			return Optional.ofNullable(latestPriorState);
		}

		@Override
		public WatermarkHandlingResult onWatermark(Watermark watermark, Collector<String> output,
				NonPartitionedContext<String> ctx) throws Exception {

			System.out.println("### AGGR - Receiving + " + watermark);

//			PartitionedContext<String> partitionedContext = getPartitionedContext(ctx);
//			System.out.println(partitionedContext.getStateManager().getState(CUSTOMER_STATE).value());

			ctx.applyToAllPartitions((collector, context) -> {
				System.out.println("### AGGR - Apply to all " + ((LongWatermark)watermark).getValue() + context.getStateManager().getState(CUSTOMER_STATE).get(((LongWatermark)watermark).getValue()));
				collector.collect(context.getStateManager().getState(CUSTOMER_STATE).get(((LongWatermark)watermark).getValue()));
				context.getStateManager().getState(CUSTOMER_STATE).remove(((LongWatermark)watermark).getValue());
			});

			return OneInputStreamProcessFunction.super.onWatermark(watermark, output, ctx);
		}

//		private <T> PartitionedContext<T> getPartitionedContext(NonPartitionedContext<T> ctx) {
//			try {
//				Field field = DefaultNonPartitionedContext.class.getDeclaredField("partitionedContext");
//				field.setAccessible(true);
//				return (PartitionedContext<T>) field.get(ctx);
//			} catch (IllegalArgumentException | IllegalAccessException | NoSuchFieldException | SecurityException e) {
//				throw new RuntimeException("Couldn't retrieve context", e);
//			}
//		}
	}

	public static class WatermarkAssignmentFunction implements OneInputStreamProcessFunction<DataChangeEvent, DataChangeEvent> {

		private static final ValueStateDeclaration<Long> TXN_STATE = StateDeclarations.valueState("last-txn", TypeDescriptors.  LONG);

		public static final LongWatermarkDeclaration WATERMARK_DECLARATION = WatermarkDeclarations
				.newBuilder("MY_CUSTOM_WATERMARK_IDENTIFIER")
				.typeLong()
				.combineFunctionMin()
				.combineWaitForAllChannels(true)
//				.defaultHandlingStrategyForward()
				.defaultHandlingStrategyIgnore()
				.build();


//		@Override
//		public Set<? extends WatermarkDeclaration> declareWatermarks() {
//			return Set.of(watermarkDeclaration);
//		}

		@Override
		public void processRecord(DataChangeEvent record, Collector<DataChangeEvent> output, PartitionedContext<DataChangeEvent> ctx) throws Exception {

			ValueState<Long> txnState = ctx.getStateManager().getState(TXN_STATE);
            Long previousTxn = txnState.value();
//			System.out.println("### Previous: " + previousTxn);

            long txId = (int)record.source().get("txId");
            if(previousTxn != null && txId != previousTxn) {
            	LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(previousTxn);
            	System.out.println("### SOURCE - Emitting + " + watermark);
            	ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
            }

            txnState.update(txId);

			output.collect(record);
		}

		@Override
		public Set<? extends WatermarkDeclaration> declareWatermarks() {
			return Set.of(WatermarkAssignmentFunction.WATERMARK_DECLARATION);
		}
	}

	public static class MyJoinFunction extends TwoInputNonBroadcastJoinProcessFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

		ValueStateDeclaration<Long> valueStateDeclaration = StateDeclarations.valueState("min-watermark-state", TypeDescriptors.LONG);

		private long minWatermarkFromFirstInput = -1;
		private long minWatermarkFromSecondInput = -1;
		private long minWatermark = -1;

		@Override
		public Set<StateDeclaration> usesStates() {
			return Set.of(valueStateDeclaration);
		}


		public MyJoinFunction(JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> joinFunction,
				JoinType joinType) {
			super(joinFunction, joinType);
		}

		@Override
		public WatermarkHandlingResult onWatermarkFromFirstInput(Watermark watermark,
				Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
				throws Exception {

//            Long previousTxn = txnState.value();

			System.out.println("### JOIN - Receiving (first) + " + watermark);

			minWatermarkFromFirstInput = ((LongWatermark)watermark).getValue();
			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);
			if (minWatermark > this.minWatermark) {
				System.out.println("### JOIN - Emitting (first) + " + watermark);

				this.minWatermark = minWatermark;
				ctx.getWatermarkManager().emitWatermark(watermark);
			}

			return WatermarkHandlingResult.POLL;
		}

		@Override
		public WatermarkHandlingResult onWatermarkFromSecondInput(Watermark watermark,
				Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
				throws Exception {

			System.out.println("### JOIN - Receiving (second) + " + watermark);

			minWatermarkFromSecondInput = ((LongWatermark)watermark).getValue();

			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);
			if (minWatermark > this.minWatermark) {
				System.out.println("### JOIN - Emitting (second) + " + watermark);

				this.minWatermark = minWatermark;
				ctx.getWatermarkManager().emitWatermark(watermark);
			}

			return WatermarkHandlingResult.POLL;
		}


	}

	public static class Joiner implements JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

		@Override
		public void processRecord(DataChangeEvent leftRecord, DataChangeEvent rightRecord, Collector<DataChangeEventPair> output,
				RuntimeContext ctx) throws Exception {


			// TODO Auto-generated method stub
			System.out.println("LEFT : " + leftRecord.after());
			System.out.println("RIGHT: " + rightRecord.after());

//			output.collect(GenericRowData.ofKind(RowKind.INSERT, leftRecord, rightRecord));
			output.collect(new DataChangeEventPair(leftRecord, rightRecord));
		}
	  }
}

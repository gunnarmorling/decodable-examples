package dev.morling.demos.txbuffering;

import static org.apache.flink.datastream.impl.utils.StreamUtils.validateStates;

import java.io.Serializable;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastStateDeclaration;
import org.apache.flink.api.common.state.MapStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.v2.ListState;
import org.apache.flink.api.common.state.v2.ListStateDescriptor;
import org.apache.flink.api.common.state.v2.MapState;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.connector.dsv2.DataStreamV2SinkUtils;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.TwoPhaseCommittingStatefulSink;
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
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.attribute.AttributeParser;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessOperator;
import org.apache.flink.datastream.impl.extension.window.function.InternalTwoInputWindowStreamProcessFunction;
import org.apache.flink.datastream.impl.operators.KeyedTwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.stream.KeyedPartitionStreamImpl;
import org.apache.flink.datastream.impl.stream.NonKeyedPartitionStreamImpl;
import org.apache.flink.datastream.impl.utils.StreamUtils;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.JsonDeserializationSchema;
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat;
import org.apache.flink.runtime.event.WatermarkEvent;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.lineage.LineageVertexProvider;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.kafka.common.Uuid;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.demos.txbuffering.TransactionEvent.Status;

public class DataStreamV2Job {

	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getInstance();
		((ExecutionEnvironmentImpl)env).getConfiguration().set(RestOptions.PORT, 8081);
		((ExecutionEnvironmentImpl)env).getConfiguration().setString("state.backend.async", "false");
		((ExecutionEnvironmentImpl)env).getConfiguration().setString("state.backend", "hashmap");

//		((ExecutionEnvironmentImpl)env).getConfiguration().set(PipelineOptions.GENERIC_TYPES, false);
//		((ExecutionEnvironmentImpl)env).getConfiguration().set(PipelineOptions.GENERIC_TYPES, false);
//
//		conf.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true)
		String bootstrapServers = "localhost:9092";

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
		JsonDeserializationSchema<TransactionEvent> txFormat=new JsonDeserializationSchema<>(TransactionEvent.class);


		KafkaSource<DataChangeEvent> customersSource = KafkaSource.<DataChangeEvent>builder()
		        .setBootstrapServers(bootstrapServers)
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


		/**
		 * {
  "status": "END",
  "id": "775:34365024",
  "event_count": 1,
  "data_collections": [
    {
      "data_collection": "inventory.customers",
      "event_count": 1
    }
  ],
  "ts_ms": 1760426071299
}
		 */


		// todo: explore watermark aligner

		KafkaSource<DataChangeEvent> ordersSource = KafkaSource.<DataChangeEvent>builder()
		        .setBootstrapServers(bootstrapServers)
//		        .setTopics("demodb.transaction")
		        .setTopics("dbserver1.inventory.orders")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
//		        .setValueOnlyDeserializer(new SimpleStringSchema())
		        .setValueOnlyDeserializer(jsonFormat)
		        .setProperty("acks", "all")
		        .build();

		KafkaSource<TransactionEvent> transactionSource = KafkaSource.<TransactionEvent>builder()
		        .setBootstrapServers(bootstrapServers)
		        .setTopics("dbserver1.transaction")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
//		        .setValueOnlyDeserializer(new SimpleStringSchema())
		        .setValueOnlyDeserializer(txFormat)
		        .setProperty("acks", "all")
		        .build();

		KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
		        .setBootstrapServers(bootstrapServers)
		        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
		            .setTopic("customers_with_orders")
		            .setValueSerializationSchema(new SimpleStringSchema())
		            .build()
		        )
		        .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
		        .build();

		@SuppressWarnings("unchecked")
		Sink<String> sink = (Sink<String>) Proxy.newProxyInstance(
				DataStreamV2Job.class.getClassLoader(),
				  new Class[] { LineageVertexProvider.class, TwoPhaseCommittingStatefulSink.class },
				  new KafkaSinkInvocationHandler(kafkaSink));


		BroadcastStream<TransactionEvent> transactionsStream = env.fromSource(DataStreamV2SourceUtils.wrapSource(transactionSource),
						"transactions-source")
		.broadcast();


		KeyedPartitionStream<Long, DataChangeEvent> customers =
				env.fromSource(
						DataStreamV2SourceUtils.wrapSource(customersSource),
						"customers-source"
						)
//				.keyBy(x -> 0)
//				.keyBy(r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("id")) : Long.valueOf((int)r.after().get("id")))
//				.withParallelism(2)
				.connectAndProcess(transactionsStream, new WatermarkInjector("inventory.customers"))
//				.withParallelism(2)
				.withName("customers-watermark-injector")
				.keyBy(r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("id")) : Long.valueOf((int)r.after().get("id")))
				;

		KeyedPartitionStream<Long, DataChangeEvent> orders =
				env.fromSource(
						DataStreamV2SourceUtils.wrapSource(ordersSource),
						"orders-source"
						)
				.connectAndProcess(transactionsStream, new WatermarkInjector("inventory.orders"))
				.withName("orders-watermark-injector")
				.keyBy(r -> r.op().equals("d") ? Long.valueOf((int)r.before().get("purchaser")) : Long.valueOf((int)r.after().get("purchaser")))
				;


//		customers.connectAndProcess(orders, new MyJoinFunction(new Joiner(), JoinType.INNER), r -> Long.valueOf((int)r.left().after().get("id")))
		//customers.connectAndProcess(orders, new MyJoinFunction(new Joiner(), JoinType.INNER))

		connectAndProcess((KeyedPartitionStreamImpl<Long, DataChangeEvent>)customers, (KeyedPartitionStreamImpl<Long, DataChangeEvent>)orders, new MyJoinFunction(new Joiner(), JoinType.INNER))

//		input.

//		BuiltinFuncs.join(
//				  customers,
//				  orders,
//				  new Joiner()
//				)
		.keyBy(r -> Long.valueOf((int)r.left().after().get("id")))
		.process(new MaterializationFunction())
//		.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>()));
		.toSink(DataStreamV2SinkUtils.wrapSink(sink));


		env.execute("my job");
	}

	public static class WatermarkInjector implements TwoInputBroadcastStreamProcessFunction<DataChangeEvent, TransactionEvent, DataChangeEvent> {

		public static final LongWatermarkDeclaration WATERMARK_DECLARATION = WatermarkDeclarations
				.newBuilder("TX_WATERMARK")
				.typeLong()
				.combineFunctionMin()
				.combineWaitForAllChannels(true)
//				.defaultHandlingStrategyForward()
				.defaultHandlingStrategyIgnore()
				.build();

		private static final String TRANSACTIONS_KEY = "TRANSACTIONS";
		private static final String CURRENT_TRANSACTION_KEY = "CURRENT_TRANSACTION";

		private static final BroadcastStateDeclaration<String, String> TRANSACTIONS_STATE = StateDeclarations.mapStateBuilder("transactions", TypeDescriptors.STRING, TypeDescriptors.STRING)
				.buildBroadcast();

		private static final BroadcastStateDeclaration<Integer, String> COUNTS_STATE = StateDeclarations.mapStateBuilder("counts", TypeDescriptors.INT, TypeDescriptors.STRING)
				.buildBroadcast();

		private transient ObjectMapper objectMapper;
		private String qualifiedTable;


		public WatermarkInjector(String qualifiedTable) {
			this.qualifiedTable = qualifiedTable;
		}

		@Override
		public void open(NonPartitionedContext<DataChangeEvent> ctx) throws Exception {
			objectMapper = new ObjectMapper();
		}

		@Override
		public void processRecordFromNonBroadcastInput(DataChangeEvent record, Collector<DataChangeEvent> output,
				PartitionedContext<DataChangeEvent> ctx) throws Exception {

//			String transactions = null; //ctx.getStateManager().getState(TRANSACTIONS_STATE).get(record.txId());

			String counts = ctx.getStateManager().getState(COUNTS_STATE).get(record.txId());

			System.out.println("### WMIN - Emitting record " + record.txId() + " - " + record.after().get("id") + " (" + qualifiedTable + ")");
			Counts countsObj;
			if (counts == null) {
				countsObj = new Counts(new HashMap<String, Integer>());
			}
			else {
				countsObj = objectMapper.readValue(counts, Counts.class);
			}
			countsObj.increment(record.qualifiedTable());
			ctx.getStateManager().getState(COUNTS_STATE).put(record.txId(), objectMapper.writeValueAsString(countsObj));

//			System.out.println("### Counts: " + countsObj);
//			System.out.println("### Transactions: " + transactions);
			output.collect(record);

			ctx.getStateManager().getState(TRANSACTIONS_STATE).put(CURRENT_TRANSACTION_KEY, String.valueOf(record.txId()));
			increaseWatermarkIfPossible(ctx);

//			if (transactions != null) {
//				TransactionEvent transactionObj = objectMapper.readValue(transactions, TransactionEvent.class);
//				if (countsObj.getCount(record.qualifiedTable()) == transactionObj.countFor(record.qualifiedTable())) {
//						LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(record.txId());
//						System.out.println("### WMIN - Emitting (On R) - " + record.txId() + " (" + qualifiedTable + ")");
//						ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
//				}
//			}

		}

		private void increaseWatermarkIfPossible(PartitionedContext<DataChangeEvent> ctx) throws Exception {
			String transactions = ctx.getStateManager().getState(TRANSACTIONS_STATE).get(TRANSACTIONS_KEY);
			Long currentTx = ctx.getStateManager().getState(TRANSACTIONS_STATE).contains(CURRENT_TRANSACTION_KEY) ? Long.valueOf(ctx.getStateManager().getState(TRANSACTIONS_STATE).get(CURRENT_TRANSACTION_KEY)) : null;

			if (transactions == null || currentTx == null) {
				return;
			}

			List<TransactionEvent> transactionsObj = objectMapper.readValue(transactions, new TypeReference<ArrayList<TransactionEvent>>() {});

			transactionsObj = pruneAndSeekToCurrentTransaction(transactionsObj, currentTx);
			if (transactionsObj == null) {
				return;
			}

			for (TransactionEvent transaction : transactionsObj) {
				String counts = ctx.getStateManager().getState(COUNTS_STATE).get(transaction.txId());
				Counts countsObj = null;
				if (counts != null) {
					countsObj = objectMapper.readValue(counts, Counts.class);
				}

				if (allEventsIngested(transaction, countsObj)) {
					LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(transaction.txId());
					System.out.println("### WMIN - Emitting WM     " + watermark.getValue() + " (" + qualifiedTable + ")");
					ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
				}
				else {
					break;
				}
			}
		}

		private boolean allEventsIngested(TransactionEvent transaction, Counts counts) {
			return transaction.countFor(qualifiedTable) == 0 ||
					(counts != null && transaction.countFor(qualifiedTable) == counts.getCount(qualifiedTable));
		}

		private List<TransactionEvent> pruneAndSeekToCurrentTransaction(List<TransactionEvent> transactionsObj, long txId) {
			Iterator<TransactionEvent> it = transactionsObj.iterator();

			while(it.hasNext()) {
				TransactionEvent next = it.next();
				if (next.txId() < txId) {
					it.remove();
				}
				else if (next.txId() == txId) {
					return transactionsObj;
				}
			}

			return null;
		}

		@Override
		public void processRecordFromBroadcastInput(TransactionEvent record, NonPartitionedContext<DataChangeEvent> ctx)
				throws Exception {

			if (record.status() == Status.END) {
				ctx.applyToAllPartitions((out, context) -> {
//					String counts = context.getStateManager().getState(COUNTS_STATE).get(record.txId());
//
//					if (counts != null) {
//						Counts countsObj = objectMapper.readValue(counts, Counts.class);
//
//						if (countsObj.getCount(qualifiedTable) == record.countFor(qualifiedTable)) {
//							LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(record.txId());
//			            	System.out.println("### WMIN - Emitting (On W) - " + record.txId() + " (" + qualifiedTable + ")");
//			            	context.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
//						}
//					}

					String transactions = context.getStateManager().getState(TRANSACTIONS_STATE).get(TRANSACTIONS_KEY);
					List<TransactionEvent> transactionsObj = null;
					if(transactions != null) {
						transactionsObj = objectMapper.readValue(transactions, new TypeReference<ArrayList<TransactionEvent>>() {});
					}
					else {
						transactionsObj = new ArrayList<>();
					}
					transactionsObj.add(record);
					context.getStateManager().getState(TRANSACTIONS_STATE).put(TRANSACTIONS_KEY, objectMapper.writeValueAsString(transactionsObj));

					increaseWatermarkIfPossible(context);
				});




//				if (record.countFor(qualifiedTable) == 0) {
//					LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(record.txId());
//	            	System.out.println("### WMIN - Emitting (On 0) - " + record.txId() + " (" + qualifiedTable + ")");
//	            	ctx.getWatermarkManager().emitWatermark(watermark);
//				}
			}

//			System.out.println("### WMIN - " + record.txId());
		}

		@Override
		public Set<? extends WatermarkDeclaration> declareWatermarks() {
			return Set.of(WATERMARK_DECLARATION);
		}
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
			System.out.println("### AGGR - Storing " + record.left().txId() + "/" + record.right().txId() + " " + record.left().after().get("id") + " - " + record.right().after().get("id"));

            MapState<Long, String> state = ctx.getStateManager().getState(CUSTOMER_STATE);

            for(Entry<Long, String> entry : state.entries()) {
            	if (entry.getKey() >= record.txId()) {
            		System.out.println("### AGGR -   Updating state for TX " + entry.getKey());
            		CustomerWithOrders customer = objectMapper.readValue(entry.getValue(), CustomerWithOrders.class);
                	customer = customer.updateFromDataChangeEventPair(record);
                	entry.setValue(objectMapper.writeValueAsString(customer));
            	}
            }

            String currentTxState = state.get(record.txId());

            if (currentTxState == null) {
            	Optional<String> latestPriorState = getLatestPriorState(record.txId(), state);

            	if(latestPriorState.isPresent()) {
            		System.out.println("### AGGR -   Seeding state from previous TX");
            		CustomerWithOrders customer = objectMapper.readValue(latestPriorState.get(), CustomerWithOrders.class);
                	customer = customer.updateFromDataChangeEventPair(record);
                	state.put(record.txId(), objectMapper.writeValueAsString(customer));
            	}
            	else {
            		System.out.println("### AGGR -   Initializing state");
            		try {
            			CustomerWithOrders customer = CustomerWithOrders.fromDataChangeEventPair(record);
            			state.put(record.txId(), objectMapper.writeValueAsString(customer));
            		}
            		catch(Exception e) {
            			e.printStackTrace();
            		}
            	}
            }
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
				String value = context.getStateManager().getState(CUSTOMER_STATE).get(((LongWatermark)watermark).getValue());
				System.out.println("### AGGR - Apply to all (" + context.getTaskInfo().getIndexOfThisSubtask() + ") " + ((LongWatermark)watermark).getValue() + " " + value);

				if (value != null) {
					collector.collect(value);
				}

//				context.getStateManager().getState(CUSTOMER_STATE).remove(((LongWatermark)watermark).getValue());
			});

			return OneInputStreamProcessFunction.super.onWatermark(watermark, output, ctx);
		}

	}


	public static class MyJoinFunction extends TwoInputNonBroadcastJoinProcessFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

		ValueStateDeclaration<Long> valueStateDeclaration = StateDeclarations.valueState("min-watermark-state", TypeDescriptors.LONG);

		private long minWatermarkFromFirstInput = -1;
		private long minWatermarkFromSecondInput = -1;
		private long minWatermark = -1;
		private SortedSet<Long> waterMarksToFlush = new TreeSet<Long>();

		@Override
		public Set<StateDeclaration> usesStates() {
			return Set.of(valueStateDeclaration);
		}


		public MyJoinFunction(JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> joinFunction,
				JoinType joinType) {
			super(joinFunction, joinType);
		}

		@Override
		public void processRecordFromFirstInput(DataChangeEvent record, Collector<DataChangeEventPair> output,
				PartitionedContext<DataChangeEventPair> ctx) throws Exception {

			System.out.println("####### RECORD " + record);

			super.processRecordFromFirstInput(record, output, ctx);
		}


		@Override
		public WatermarkHandlingResult onWatermarkFromFirstInput(Watermark watermark,
				Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
				throws Exception {

//            Long previousTxn = txnState.value();

			System.out.println("### JOINF - Receiving WM    " + ((LongWatermark)watermark).getValue() + " (first)");
			waterMarksToFlush.add(((LongWatermark)watermark).getValue());
			minWatermarkFromFirstInput = ((LongWatermark)watermark).getValue();

			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

//			System.out.println(this.minWatermark + " " + minWatermarkFromFirstInput + " " + minWatermarkFromSecondInput);

			if (minWatermark > this.minWatermark) {
				this.minWatermark = minWatermark;

				Iterator<Long> waterMarkstoFlush = waterMarksToFlush.iterator();

				while(waterMarkstoFlush.hasNext()) {
					Long toFlush = waterMarkstoFlush.next();

					if (toFlush <= minWatermark) {
						waterMarkstoFlush.remove();

						System.out.println("### JOINF -   Emitting WM - " + toFlush + " (first)");
						LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(toFlush);
						ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
					}
					else {
						break;
					}
				}
			}

			return WatermarkHandlingResult.POLL;
		}

		@Override
		public WatermarkHandlingResult onWatermarkFromSecondInput(Watermark watermark,
				Collector<DataChangeEventPair> output, NonPartitionedContext<DataChangeEventPair> ctx)
				throws Exception {

			System.out.println("### JOINF - Receiving WM    " + ((LongWatermark)watermark).getValue() + " (second)");
			waterMarksToFlush.add(((LongWatermark)watermark).getValue());
			minWatermarkFromSecondInput = ((LongWatermark)watermark).getValue();

			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

			if (minWatermark > this.minWatermark) {
				this.minWatermark = minWatermark;

				Iterator<Long> waterMarkstoFlush = waterMarksToFlush.iterator();

				while(waterMarkstoFlush.hasNext()) {
					Long toFlush = waterMarkstoFlush.next();

					if (toFlush <= minWatermark) {
						waterMarkstoFlush.remove();

						System.out.println("### JOINF -   Emitting WM - " + toFlush + " (second)");
						LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(toFlush);
						ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
					}
					else {
						break;
					}
				}
			}


			return WatermarkHandlingResult.POLL;
		}


	}

	public static class Joiner implements JoinFunction<DataChangeEvent, DataChangeEvent, DataChangeEventPair> {

		@Override
		public void processRecord(DataChangeEvent leftRecord, DataChangeEvent rightRecord, Collector<DataChangeEventPair> output,
				RuntimeContext ctx) throws Exception {

			System.out.println("### JOIN - " + leftRecord.txId() + "/" + rightRecord.txId() + " " + leftRecord.after().get("id") + " - " + rightRecord.after().get("id"));

			// TODO Auto-generated method stub
//			System.out.println("LEFT : " + leftRecord.after());
//			System.out.println("RIGHT: " + rightRecord.after());

//			output.collect(GenericRowData.ofKind(RowKind.INSERT, leftRecord, rightRecord));
			output.collect(new DataChangeEventPair(leftRecord, rightRecord));
		}
	}

	public static class KafkaSinkInvocationHandler implements InvocationHandler, Serializable {

		private final KafkaSink<?> delegate;

		public KafkaSinkInvocationHandler(KafkaSink<?> delegate) {
			this.delegate = delegate;
		}

		@Override
		public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
			return method.invoke(delegate, args);
		}
	}

	private static <K, V, T_OTHER, OUT> ProcessConfigurableAndNonKeyedPartitionStream<OUT> connectAndProcess(
			KeyedPartitionStreamImpl<K, V> first,
            KeyedPartitionStreamImpl<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction) {
        validateStates(
                processFunction.usesStates(),
                new HashSet<>(
                        Collections.singletonList(StateDeclaration.RedistributionMode.IDENTICAL)));
//        other =
//                other instanceof ProcessConfigurableAndKeyedPartitionStreamImpl
//                        ? ((ProcessConfigurableAndKeyedPartitionStreamImpl) other)
//                                .getKeyedPartitionStream()
//                        : other;
        TypeInformation<OUT> outTypeInfo =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        processFunction,
                        first.getType(),
                        other.getType());

        Transformation<OUT> outTransformation;

        if (processFunction instanceof TwoInputNonBroadcastJoinProcessFunction) {
            outTransformation = getJoinTransformation(first, other, processFunction, outTypeInfo);
        } else if (processFunction instanceof InternalTwoInputWindowStreamProcessFunction) {
            outTransformation =
                    StreamUtils.transformTwoInputNonBroadcastWindow(
                            first.getEnvironment().getExecutionConfig(),
                            first.getTransformation(),
                            first.getType(),
                            other.getTransformation(),
                            other.getType(),
                            outTypeInfo,
                            (InternalTwoInputWindowStreamProcessFunction<V, T_OTHER, OUT, ?>)
                                    processFunction,
                            first.getKeySelector(),
                            first.getKeyType(),
                            other.getKeySelector(),
                            other.getKeyType());
        } else {
            KeyedTwoInputNonBroadcastProcessOperator<K, V, T_OTHER, OUT> processOperator =
                    new KeyedTwoInputNonBroadcastProcessOperator<>(processFunction);
            outTransformation =
                    StreamUtils.getTwoInputTransformation(
                            "Keyed-TwoInput-Process",
                            first,
                            other,
                            outTypeInfo,
                            processOperator);
        }

        outTransformation.setAttribute(AttributeParser.parseAttribute(processFunction));
        first.getEnvironment().addOperator(outTransformation);
        return StreamUtils.wrapWithConfigureHandle(
                new NonKeyedPartitionStreamImpl<>(first.getEnvironment(), outTransformation));
    }

	private static <K, V, T_OTHER, OUT> Transformation<OUT> getJoinTransformation(
			KeyedPartitionStreamImpl<K, V> first,
            KeyedPartitionStream<K, T_OTHER> other,
            TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> processFunction,
            TypeInformation<OUT> outTypeInfo) {
        ListStateDescriptor<V> leftStateDesc =
                new ListStateDescriptor<>("join-left-state", first.getType());
        ListStateDescriptor<T_OTHER> rightStateDesc =
                new ListStateDescriptor<>(
                        "join-right-state",
                        ((KeyedPartitionStreamImpl<Object, T_OTHER>) other).getType());
        TwoInputNonBroadcastJoinProcessOperator<K, V, T_OTHER, OUT> joinProcessOperator =
                new TxAwareTwoInputNonBroadcastJoinProcessOperator<>(
                        processFunction, leftStateDesc, rightStateDesc);
        return StreamUtils.getTwoInputTransformation(
                "Keyed-Join-Process",
                first,
                (KeyedPartitionStreamImpl<K, T_OTHER>) other,
                outTypeInfo,
                joinProcessOperator);
    }

	private static class TxAwareTwoInputNonBroadcastJoinProcessOperator<K, V, T_OTHER, OUT> extends TwoInputNonBroadcastJoinProcessOperator<K, V, T_OTHER, OUT> {

		private final ListStateDescriptor<V> leftStateDescriptor;

	    private final ListStateDescriptor<T_OTHER> rightStateDescriptor;

	    /** The state that stores the left input records. */
	    private transient ListState<V> leftState1;

	    /** The state that stores the right input records. */
	    private transient ListState<T_OTHER> rightState1;

		private long minWatermarkFromFirstInput = -1;
		private long minWatermarkFromSecondInput = -1;
		private long minWatermark = -1;

		private SortedSet<Long> watermarksToFlush = new TreeSet<Long>();

		public TxAwareTwoInputNonBroadcastJoinProcessOperator(
				TwoInputNonBroadcastStreamProcessFunction<V, T_OTHER, OUT> userFunction,
				ListStateDescriptor<V> leftStateDescriptor, ListStateDescriptor<T_OTHER> rightStateDescriptor) {
			super(userFunction, leftStateDescriptor, rightStateDescriptor);

			this.leftStateDescriptor =
	                new ListStateDescriptor<>("buffer-left-state", leftStateDescriptor.getTypeInformation());
			this.rightStateDescriptor =
	                new ListStateDescriptor<>("buffer-right-state", rightStateDescriptor.getTypeInformation());
		}

		@Override
		public void open() throws Exception {
			super.open();

			System.out.println(getAsyncKeyedStateBackend());

			leftState1 =
					getOrCreateKeyedState(
							VoidNamespace.INSTANCE,
							VoidNamespaceSerializer.INSTANCE,
							leftStateDescriptor);
			rightState1 =
					getOrCreateKeyedState(
							VoidNamespace.INSTANCE,
							VoidNamespaceSerializer.INSTANCE,
							rightStateDescriptor);
		}

//		@Override
//		public void processWatermark1Internal(WatermarkEvent watermark) throws Exception {
//			minWatermarkFromFirstInput = ((LongWatermark)watermark.getWatermark()).getValue();
//			System.out.println("### JOIN - Receiving WM   " + minWatermarkFromFirstInput + " (first)");
//
//			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);
//
//			if (minWatermark > this.minWatermark) {
//				this.minWatermark = minWatermark;
//				flushBuffers(minWatermark);
//			}
//
//			super.processWatermark1Internal(watermark);
//		}
//
//		@Override
//		public void processWatermark2Internal(WatermarkEvent watermark) throws Exception {
//			minWatermarkFromSecondInput = ((LongWatermark)watermark.getWatermark()).getValue();
//			System.out.println("### JOIN - Receiving WM   " + minWatermarkFromSecondInput + " (second)");
//
//			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);
//
//			if (minWatermark > this.minWatermark) {
//				this.minWatermark = minWatermark;
//				flushBuffers(minWatermark);
//			}
//
//			super.processWatermark2Internal(watermark);
//		}

		@Override
		public void processWatermark1Internal(WatermarkEvent watermark) throws Exception {
			System.out.println("### JOIN - Receiving WM   " + ((LongWatermark)watermark.getWatermark()).getValue() + " (first)");

			minWatermarkFromFirstInput = ((LongWatermark)watermark.getWatermark()).getValue();
			watermarksToFlush.add(minWatermarkFromFirstInput);


			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

			if (minWatermark > this.minWatermark) {
				this.minWatermark = minWatermark;
				flushBuffers(minWatermark);
			}

			super.processWatermark1Internal(watermark);
		}

		@Override
		public void processWatermark2Internal(WatermarkEvent watermark) throws Exception {
			System.out.println("### JOIN - Receiving WM   " + ((LongWatermark)watermark.getWatermark()).getValue() + " (second)");

			minWatermarkFromSecondInput = ((LongWatermark)watermark.getWatermark()).getValue();
			watermarksToFlush.add(minWatermarkFromSecondInput);

			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

			if (minWatermark > this.minWatermark) {
				this.minWatermark = minWatermark;
				flushBuffers(minWatermark);
			}

			super.processWatermark2Internal(watermark);
		}

		private void flushBuffers(long watermark) throws Exception {
			Iterator<Long> waterMarkstoFlush = watermarksToFlush.iterator();

			while(waterMarkstoFlush.hasNext()) {
				Long toFlush = waterMarkstoFlush.next();

				if(toFlush <= watermark) {

					System.out.println("### JOIN -   Flushing buffers - " + toFlush);
					waterMarkstoFlush.remove();

					Set<Object> keys = keySet;
		//
					keys.forEach(key -> {
						try {
							setAsyncKeyedContextElement(new StreamRecord<Long>((Long) key), r -> r);
						} catch (Exception e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						}
						Iterable<V> left = leftState1.get();

						if (left != null) {
							for(V record : left) {
								try {
									DataChangeEvent dce = (DataChangeEvent) record;
									if (dce.txId() <= toFlush) {
										super.processElement1(new StreamRecord<>(record));
									}
								} catch (Exception e) {
									throw new RuntimeException(e);
								}
							}

//							leftState1.clear();
						}

						Iterable<T_OTHER> right = rightState1.get();

						if (right != null) {
							for(T_OTHER record : right) {
								try {
									DataChangeEvent dce = (DataChangeEvent) record;
									if (dce.txId() <= toFlush) {
										super.processElement2(new StreamRecord<>(record));
									}
								} catch (Exception e) {
									throw new RuntimeException(e);
								}
							}

//							rightState1.clear();$
						}

					});
				}
				else {
					break;
				}
			}
		}

//		@Override
//		public void processWatermark1(org.apache.flink.streaming.api.watermark.Watermark mark) throws Exception {
//			// TODO Auto-generated method stub
//			super.processWatermark1(mark);
//		}

		@Override
		public void processElement1(StreamRecord<V> element) throws Exception {
			V leftRecord = element.getValue();
	        leftState1.add(leftRecord);
		}

		@Override
		public void processElement2(StreamRecord<T_OTHER> element) throws Exception {
			T_OTHER rightRecord = element.getValue();
	        rightState1.add(rightRecord);
		}
	}
}

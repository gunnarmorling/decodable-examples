package dev.morling.demos.txbuffering.simple;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.flink.api.common.state.BroadcastStateDeclaration;
import org.apache.flink.api.common.state.StateDeclaration;
import org.apache.flink.api.common.state.StateDeclarations;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.typeinfo.TypeDescriptors;
import org.apache.flink.api.common.watermark.LongWatermark;
import org.apache.flink.api.common.watermark.LongWatermarkDeclaration;
import org.apache.flink.api.common.watermark.Watermark;
import org.apache.flink.api.common.watermark.WatermarkDeclaration;
import org.apache.flink.api.common.watermark.WatermarkDeclarations;
import org.apache.flink.api.common.watermark.WatermarkHandlingResult;
import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.RuntimeContext;
import org.apache.flink.datastream.api.extension.join.JoinFunction;
import org.apache.flink.datastream.api.extension.join.JoinType;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.stream.BroadcastStream;
import org.apache.flink.datastream.api.stream.KeyedPartitionStream;
import org.apache.flink.datastream.impl.extension.join.operators.TwoInputNonBroadcastJoinProcessFunction;

import com.fasterxml.jackson.databind.ObjectMapper;

import dev.morling.demos.txbuffering.Counts;

public class TxAwareJoinJoin {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getInstance();

		BroadcastStream<Transaction> transactions =
		env.fromSource(
				DataStreamV2SourceUtils.fromData(
						List.of(
								"{ \"txId\" : 1, \"counts\" : [ { \"collection\" : \"purchase-orders\", \"count\" : 3 }, { \"collection\" : \"order-lines\", \"count\" : 2 } ] }",
								"{ \"txId\" : 2, \"counts\" : [ { \"collection\" : \"purchase-orders\", \"count\" : 2 }, { \"collection\" : \"order-lines\", \"count\" : 1 } ] }",
								"{ \"txId\" : 3, \"counts\" : [ { \"collection\" : \"purchase-orders\", \"count\" : 1 }, { \"collection\" : \"order-lines\", \"count\" : 0 } ] }"
						)),
				"tx-source"
				)
			.process(new TransactionMarshaller())
			.broadcast();

		KeyedPartitionStream<Long, PurchaseOrder> purchaseOrders =
				env.fromSource(
						DataStreamV2SourceUtils.fromData(
								List.of(
										"{ \"txId\" : 1, \"id\" : 1, \"description\" : \"aaa\" }",
										"{ \"txId\" : 1, \"id\" : 2, \"description\" : \"bbb\" }",
										"{ \"txId\" : 1, \"id\" : 3, \"description\" : \"ccc\" }",
										"{ \"txId\" : 2, \"id\" : 4, \"description\" : \"ddd\" }",
										"{ \"txId\" : 2, \"id\" : 5, \"description\" : \"eee\" }",
										"{ \"txId\" : 3, \"id\" : 6, \"description\" : \"fff\" }"
										)),
						"orders-source"
						)
				.process(new PurchaseOrderMarshaller())
				.connectAndProcess(transactions, new WatermarkInjector<>("purchase-orders"))
				.keyBy(o -> o.id());

		KeyedPartitionStream<Long, OrderLine> orderLines =
				env.fromSource(
						DataStreamV2SourceUtils.fromData(
								List.of(
										"{ \"txId\" : 1, \"id\" : 11, \"orderId\" : 1, \"description\" : \"aaa1\" }",
										"{ \"txId\" : 1, \"id\" : 12, \"orderId\" : 1, \"description\" : \"aaa2\" }",
										"{ \"txId\" : 2, \"id\" : 13, \"orderId\" : 4, \"description\" : \"ddd1\" }"
										)),
						"lines-source"
						)
				.process(new OrderLineMarshaller())
				.connectAndProcess(transactions, new WatermarkInjector<>("order-lines"))
				.keyBy(l -> l.orderId());

		purchaseOrders
			.connectAndProcess(orderLines, new MyJoinFunction(new Joiner(), JoinType.INNER))
			.process(new OutputHandler());
//			.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>()));

		env.execute("my job");
	}

	private static class Marshaller<T> implements OneInputStreamProcessFunction<String, T> {

		private transient ObjectMapper objectMapper;

		private final Class<T> type;

		public Marshaller(Class<T> type) {
			this.type = type;
		}

		@Override
		public void open(NonPartitionedContext<T> ctx) throws Exception {
			objectMapper = new ObjectMapper();
		}

		@Override
		public void processRecord(String record, Collector<T> output, PartitionedContext<T> ctx)
				throws Exception {

			T obj = objectMapper.readValue(record, type);
			output.collect(obj);
		}
	}

	private static class PurchaseOrderMarshaller extends Marshaller<PurchaseOrder> {

		public PurchaseOrderMarshaller() {
			super(PurchaseOrder.class);
		}
	}

	private static class OrderLineMarshaller extends Marshaller<OrderLine> {

		public OrderLineMarshaller() {
			super(OrderLine.class);
		}
	}

	private static class TransactionMarshaller extends Marshaller<Transaction> {

		public TransactionMarshaller() {
			super(Transaction.class);
		}
	}


	private static class WatermarkInjector<T extends TransactionAware> implements TwoInputBroadcastStreamProcessFunction<T, Transaction, T> {

		public static final LongWatermarkDeclaration WATERMARK_DECLARATION = WatermarkDeclarations
				.newBuilder("TX_WATERMARK")
				.typeLong()
				.combineFunctionMin()
				.combineWaitForAllChannels(true)
				.defaultHandlingStrategyIgnore()
				.build();

//		private static ValueStateDeclaration<Long> PREVIOUS_TX = StateDeclarations.valueState("previous-tx", TypeDescriptors.LONG);

		private static final BroadcastStateDeclaration<Long, String> TRANSACTIONS_STATE = StateDeclarations.mapStateBuilder("transactions", TypeDescriptors.LONG, TypeDescriptors.STRING)
				.buildBroadcast();
		private static final BroadcastStateDeclaration<Long, String> COUNTS_STATE = StateDeclarations.mapStateBuilder("counts", TypeDescriptors.LONG, TypeDescriptors.STRING)
				.buildBroadcast();

		private transient ObjectMapper objectMapper;

		private final String table;

		public WatermarkInjector(String table) {
			this.table = table;
		}

		@Override
		public void open(NonPartitionedContext<T> ctx) throws Exception {
			objectMapper = new ObjectMapper();
		}

		@Override
		public void processRecordFromNonBroadcastInput(T record, Collector<T> output,
				PartitionedContext<T> ctx) throws Exception {

			String transactions = ctx.getStateManager().getState(TRANSACTIONS_STATE).get(record.txId());
			String counts = ctx.getStateManager().getState(COUNTS_STATE).get(record.txId());

			Counts countsObj;
			if (counts == null) {
				countsObj = new Counts(new HashMap<String, Integer>());
			}
			else {
				countsObj = objectMapper.readValue(counts, Counts.class);
			}
			countsObj.increment(table);
			ctx.getStateManager().getState(COUNTS_STATE).put(record.txId(), objectMapper.writeValueAsString(countsObj));

			System.out.println("### WMIN: Emitting record    - " + record);
			output.collect(record);

			if (transactions != null) {
				Transaction transactionObj = objectMapper.readValue(transactions, Transaction.class);
				if (countsObj.getCount(table) == transactionObj.countFor(table)) {
						LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(record.txId());
						System.out.println("### WMIN: Emitting WM (On R) - " + watermark + " (" + table + ")");
						ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
				}
			}
		}

		@Override
		public void processRecordFromBroadcastInput(Transaction record, NonPartitionedContext<T> ctx)
				throws Exception {

			System.out.println("### WMIN: Receiving TX       - " + record + " (" + table + ")");

			ctx.applyToAllPartitions((out, context) -> {
				String counts = context.getStateManager().getState(COUNTS_STATE).get(record.txId());

				if (counts != null) {
					Counts countsObj = objectMapper.readValue(counts, Counts.class);

					if (countsObj.getCount(table) == record.countFor(table)) {
						LongWatermark watermark = WATERMARK_DECLARATION.newWatermark(record.txId());
		            	System.out.println("### WMIN: Emitting WM (On TX) - " + watermark + " (" + table + ")");
		            	context.getNonPartitionedContext().getWatermarkManager().emitWatermark(watermark);
					}
				}

				context.getStateManager().getState(TRANSACTIONS_STATE).put(record.txId(), objectMapper.writeValueAsString(record));
			});


		}


//		@Override
//		public void processRecord(PurchaseOrder record, Collector<PurchaseOrder> output, PartitionedContext<PurchaseOrder> ctx)
//				throws Exception {
//
//
//			System.out.println("### WI - REC - " + record);
//
//			Long previousTx = ctx.getStateManager().getState(PREVIOUS_TX).value();
//
//			if(previousTx != null && previousTx != record.txId()) {
//				System.out.println("### WI - WM - " + record);
//				ctx.getNonPartitionedContext().getWatermarkManager().emitWatermark(WATERMARK_DECLARATION.newWatermark(previousTx));
//			}
//
//			ctx.getStateManager().getState(PREVIOUS_TX).update(record.txId());
//			output.collect(record);
//		}

		@Override
		public Set<? extends WatermarkDeclaration> declareWatermarks() {
			return Set.of(WATERMARK_DECLARATION);
		}
	}

	public static class MyJoinFunction extends TwoInputNonBroadcastJoinProcessFunction<PurchaseOrder, OrderLine, PurchaseOrderWithLine> {

		ValueStateDeclaration<Long> valueStateDeclaration = StateDeclarations.valueState("min-watermark-state", TypeDescriptors.LONG);

		private long minWatermarkFromFirstInput = -1;
		private long minWatermarkFromSecondInput = -1;
		private long minWatermark = -1;

		@Override
		public Set<StateDeclaration> usesStates() {
			return Set.of(valueStateDeclaration);
		}


		public MyJoinFunction(JoinFunction<PurchaseOrder, OrderLine, PurchaseOrderWithLine> joinFunction,
				JoinType joinType) {
			super(joinFunction, joinType);
		}

		@Override
		public WatermarkHandlingResult onWatermarkFromFirstInput(Watermark watermark,
				Collector<PurchaseOrderWithLine> output, NonPartitionedContext<PurchaseOrderWithLine> ctx)
				throws Exception {

//            Long previousTxn = txnState.value();

			System.out.println("### JOIN: Receiving (first)  - " + watermark);

			minWatermarkFromFirstInput = ((LongWatermark)watermark).getValue();

			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

//			System.out.println(this.minWatermark + " " + minWatermarkFromFirstInput + " " + minWatermarkFromSecondInput);

			if (minWatermark > this.minWatermark) {
				System.out.println("### JOIN: Emitting (first)   - " + watermark);

				this.minWatermark = minWatermark;
				LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(minWatermark);
				ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
			}

			return WatermarkHandlingResult.POLL;
		}

		@Override
		public WatermarkHandlingResult onWatermarkFromSecondInput(Watermark watermark,
				Collector<PurchaseOrderWithLine> output, NonPartitionedContext<PurchaseOrderWithLine> ctx)
				throws Exception {

			System.out.println("### JOIN: Receiving (second) - " + watermark);

			minWatermarkFromSecondInput = ((LongWatermark)watermark).getValue();

			long minWatermark = Math.min(minWatermarkFromFirstInput, minWatermarkFromSecondInput);

//			System.out.println(this.minWatermark + " " + minWatermarkFromFirstInput + " " + minWatermarkFromSecondInput);

			if (minWatermark > this.minWatermark) {
				System.out.println("### JOIN: Emitting (second) - " + watermark);

				this.minWatermark = minWatermark;
				LongWatermark outgoingWatermark = WatermarkInjector.WATERMARK_DECLARATION.newWatermark(minWatermark);
				ctx.getWatermarkManager().emitWatermark(outgoingWatermark);
			}

			return WatermarkHandlingResult.POLL;
		}


	}

	public static class Joiner implements JoinFunction<PurchaseOrder, OrderLine, PurchaseOrderWithLine> {

		@Override
		public void processRecord(PurchaseOrder leftRecord, OrderLine rightRecord, Collector<PurchaseOrderWithLine> output,
				RuntimeContext ctx) throws Exception {

			System.out.println("### JOIN: Receiving record   - " + leftRecord + " /" + rightRecord);

			// TODO Auto-generated method stub
//			System.out.println("LEFT : " + leftRecord.after());
//			System.out.println("RIGHT: " + rightRecord.after());

//			output.collect(GenericRowData.ofKind(RowKind.INSERT, leftRecord, rightRecord));
			output.collect(new PurchaseOrderWithLine(leftRecord.txId(), leftRecord.id(), leftRecord.description(), rightRecord.id(), rightRecord.description()));
		}
	}

	private static class OutputHandler implements OneInputStreamProcessFunction<PurchaseOrderWithLine, PurchaseOrderWithLine> {

		private static ValueStateDeclaration<Long> PREVIOUS_TX = StateDeclarations.valueState("previous-tx", TypeDescriptors.LONG);

		@Override
		public void processRecord(PurchaseOrderWithLine record, Collector<PurchaseOrderWithLine> output, PartitionedContext<PurchaseOrderWithLine> ctx)
				throws Exception {

			System.out.println("### OUTH: Receiving record   - " + record);
			output.collect(record);
		}

		@Override
		public WatermarkHandlingResult onWatermark(Watermark watermark, Collector<PurchaseOrderWithLine> output,
				NonPartitionedContext<PurchaseOrderWithLine> ctx) throws Exception {

			System.out.println("### OUTH: Receiving WM       - " + watermark);

			return WatermarkHandlingResult.POLL;
		}
	}
}

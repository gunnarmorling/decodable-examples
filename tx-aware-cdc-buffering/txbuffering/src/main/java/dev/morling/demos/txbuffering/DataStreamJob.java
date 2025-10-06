/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package dev.morling.demos.txbuffering;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.formats.common.TimestampFormat;
import org.apache.flink.formats.json.debezium.DebeziumJsonDecodingFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.kafka.common.Uuid;

/**
 * Skeleton for a Flink DataStream Job.
 *
 * <p>For a tutorial how to write a Flink application, check the
 * tutorials and examples on the <a href="https://flink.apache.org">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class DataStreamJob {

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		WatermarkStrategy<RowData> watermarkStrategy = WatermarkStrategy.forGenerator(c -> {
			return new WatermarkGenerator<RowData>() {

				@Override
				public void onEvent(RowData event, long eventTimestamp, WatermarkOutput output) {
//					System.out.printl("#### Event: " + event);
					output.emitWatermark(new Watermark(eventTimestamp));

				}

				@Override
				public void onPeriodicEmit(WatermarkOutput output) {
					// TODO Auto-generated method stub

				}
			};
		}).withIdleness(Duration.ofSeconds(5));

		watermarkStrategy = WatermarkStrategy.<RowData>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withIdleness(Duration.ofSeconds(5));

		String bootstrap_servers = "localhost:9092";

		DebeziumJsonDecodingFormat decodingFormat = new DebeziumJsonDecodingFormat(true, false, TimestampFormat.ISO_8601);
		decodingFormat.applyReadableMetadata(List.of("source.properties"));

		DataType physicalDataType = DataTypes.ROW(
	            DataTypes.FIELD("id", DataTypes.BIGINT()),
	            DataTypes.FIELD("first_name", DataTypes.STRING()),
	            DataTypes.FIELD("last_name", DataTypes.STRING()),
	            DataTypes.FIELD("email", DataTypes.STRING())
	        );

		DeserializationSchema<RowData> decoder = decodingFormat.createRuntimeDecoder(ScanRuntimeProviderContext.INSTANCE, physicalDataType);


		KafkaSource<RowData> ksource = KafkaSource.<RowData>builder()
		        .setBootstrapServers(bootstrap_servers)
//		        .setTopics("demodb.transaction")
		        .setTopics("demodb.inventory.customers")
		        .setGroupId(Uuid.randomUuid().toString())
		        .setStartingOffsets(OffsetsInitializer.earliest())
//		        .setValueOnlyDeserializer(new SimpleStringSchema())
		        .setValueOnlyDeserializer(decoder)
		        .setProperty("acks", "all")
		        .build();


		DataStream<RowData> stream = env.fromSource(
			    ksource,
			    watermarkStrategy,
			    "Redpanda Source"
			);

		TypeInformation<?>[] types = {
				BasicTypeInfo.LONG_TYPE_INFO,
			    BasicTypeInfo.STRING_TYPE_INFO,
			    BasicTypeInfo.STRING_TYPE_INFO,
			    BasicTypeInfo.STRING_TYPE_INFO,
			    BasicTypeInfo.INSTANT_TYPE_INFO
			};

		RowTypeInfo rowTypeInfo = new RowTypeInfo(
			    types,
			    new String[]{"id", "first_name", "last_name", "email", "event_time"}
			);



		DataStream<Row> rowStream = stream
				.process(new MyProcessFunction())
				.map(r -> {


			System.out.println("#### Map: " + r);
			Row row = Row.withNames(r.getRowKind());
			row.setField("id", r.getLong(0));
			row.setField("first_name", r.getString(1).toString());
			row.setField("last_name", r.getString(2).toString());
			row.setField("email", r.getString(3).toString());
			row.setField("event_time", Instant.now());
			for(int i = 0; i < r.getMap(4).keyArray().size(); i++) {
				System.out.println(r.getMap(4).keyArray().getString(i));

			}
			return row;
		})
				.returns(rowTypeInfo);
//
		StreamTableEnvironment ste = StreamTableEnvironment.create(env);
		Schema schema = Schema.newBuilder()
				.column("id", DataTypes.BIGINT())
				.column("first_name", DataTypes.STRING())
				.column("last_name", DataTypes.STRING())
				.column("email", DataTypes.STRING())
				.column("event_time", DataTypes.TIMESTAMP_LTZ(3))
				.watermark("event_time", "SOURCE_WATERMARK()")
				.build();

		Table table = ste.fromChangelogStream(rowStream, schema);
//
		ste.createTemporaryView("InputTable", table);
		ste
		    .executeSql("SELECT id, first_name FROM InputTable")
		    .print();




//		DataStream<Row> dataStream =
//			    env.fromElements(
//			        Row.ofKind(RowKind.INSERT, "Alice", 12),
//			        Row.ofKind(RowKind.INSERT, "Bob", 5),
//			        Row.ofKind(RowKind.UPDATE_BEFORE, "Alice", 12),
//			        Row.ofKind(RowKind.UPDATE_AFTER, "Alice", 100)).returns(rowTypeInfo);


//		 Schema schema = Schema.newBuilder()
//         .column("name", "STRING")
//         .column("score", "BIGINT")
//         .build();

			// interpret the DataStream as a Table
//			Table table = ste.fromChangelogStream(dataStream);

			// register the table under a name and perform an aggregation
//			ste.createTemporaryView("InputTable", table);
//			ste
//			    .executeSql("SELECT name, score FROM InputTable ")
//			    .print();



//		PrintSink<RowData> sink = new PrintSink<>(true);
//
//		stream = stream.map(r -> {
//			System.out.println("#### Map: " + r);
//			return r;
//		});
//
//		stream.sinkTo(sink);

		// Execute program, beginning computation.
		env.execute("Flink Java API Skeleton");
	}

	public static class MyProcessFunction extends ProcessFunction<RowData, RowData> {

		@Override
		public void processElement(RowData row, ProcessFunction<RowData, RowData>.Context context,
				Collector<RowData> out) throws Exception {

			System.out.println("#### PROCESS: " + row);
			System.out.println("#### PROCESS: " + context.timestamp());
			System.out.println("#### PROCESS: " + context.timerService().currentWatermark());


		}

	}
}

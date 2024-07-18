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

package flinkExplore;

import objects.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import serdes.JSONDeserializationSchema;
import serdes.JSONSerializationSchema;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

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
	final static String CONFIG_FILENAME = "config/config.properties";

//	public static Properties readPropertiesFile(String fileName) {
//		FileInputStream fis = null;
//		Properties props = null;
//
//		try {
//			fis = new FileInputStream(fileName);
//			props = new Properties();
//			props.load(fis);
//			fis.close();
//		} catch (FileNotFoundException e) {
//            throw new RuntimeException(e);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//
//		return props;
//    }

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		/*
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.fromSequence(1, 10);
		 *
		 * then, transform the resulting DataStream<Long> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.window()
		 * 	.process()
		 *
		 * and many more.
		 * Have a look at the programming guide:
		 *
		 * https://nightlies.apache.org/flink/flink-docs-stable/
		 *
		 */

		// Kafka Constants
		final String KAFKA_BOOTRSTRAP_SERVER = "broker:29092";
		final String KAFKA_TOPIC_INPUT = "customer_transactions";
		final String KAFKA_TOPIC_OUTPUT = "customer_transactions_current_state";
		final String FLINK_GROUP_ID = "FLINK_GROUP_ID";

		// Setup KafkaSource
		KafkaSource<Transaction> source = KafkaSource.<Transaction>builder()
				.setBootstrapServers(KAFKA_BOOTRSTRAP_SERVER)
				.setTopics(KAFKA_TOPIC_INPUT)
				.setGroupId(FLINK_GROUP_ID)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new JSONDeserializationSchema())
				.build();

		DataStream<Transaction> transactionStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Topic Customer Transactions") ;
		transactionStream.print();

		// Setup KafkaSink
		KafkaSink<Transaction> sink = KafkaSink.<Transaction>builder()
				.setBootstrapServers(KAFKA_BOOTRSTRAP_SERVER)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(KAFKA_TOPIC_OUTPUT)
						.setValueSerializationSchema(new JSONSerializationSchema())
						.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		// Stream to sink
		transactionStream.sinkTo(sink);

		// Execute program, beginning computation.
		env.execute("Kafka Flink Explore Job started!");
	}
}

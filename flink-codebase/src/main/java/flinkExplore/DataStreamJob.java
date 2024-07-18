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

import objects.Customer;
import objects.Transaction;
import objects.CustomerTransaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;
import serdes.CustomerJSONDeserializationSchema;
import serdes.CustomerTransactionJSONSerializationSchema;
import serdes.TransactionJSONDeserializationSchema;
import serdes.JSONSerializationSchema;

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
		final String KAFKA_BOOTRSTRAP_SERVER = "localhost:9092";
		final String KAFKA_TOPIC_TRANSACTIONS = "transactions_topic";
		final String KAFKA_TOPIC_CUSTOMER = "customer_topic";
		final String KAFKA_TOPIC_OUTPUT = "customer_transactions_topic";
		final String FLINK_GROUP_ID = "FLINK_GROUP_ID";

		// Setup Kafka Source's
		KafkaSource<Transaction> transactionSource = KafkaSource.<Transaction>builder()
				.setBootstrapServers(KAFKA_BOOTRSTRAP_SERVER)
				.setTopics(KAFKA_TOPIC_TRANSACTIONS)
				.setGroupId(FLINK_GROUP_ID)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new TransactionJSONDeserializationSchema())
				.build();

		KafkaSource<Customer> customerSource = KafkaSource.<Customer>builder()
				.setBootstrapServers(KAFKA_BOOTRSTRAP_SERVER)
				.setTopics(KAFKA_TOPIC_CUSTOMER)
				.setGroupId(FLINK_GROUP_ID)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new CustomerJSONDeserializationSchema())
				.build();

		// Setup KafkaSink
		KafkaSink<CustomerTransaction> sink = KafkaSink.<CustomerTransaction>builder()
				.setBootstrapServers(KAFKA_BOOTRSTRAP_SERVER)
				.setRecordSerializer(KafkaRecordSerializationSchema.builder()
						.setTopic(KAFKA_TOPIC_OUTPUT)
						.setValueSerializationSchema(new CustomerTransactionJSONSerializationSchema())
						.build())
				.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
				.build();

		// DataStreams Setup
		DataStream<Transaction> transactionStream = env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Kafka Transactions").keyBy(transaction -> transaction.getCustomerId());
		DataStream<Customer> customerStream = env.fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Kafka Transactions").keyBy(customer -> customer.getCustomerId()) ;

		// Join Transaction on customer to enrich data
		transactionStream.connect(customerStream).flatMap(new EnrichmentFunction()).sinkTo(sink);

		transactionStream.print();

		// Execute program, beginning computation.
		env.execute("Kafka Flink Explore Job started!");
	}
	
	public static class EnrichmentFunction
			extends RichCoFlatMapFunction<Transaction, Customer, CustomerTransaction> {

		// Defining the states
		private ValueState<Transaction> transactionState;
		private ValueState<Customer> customerState;

		@Override
		public void open(Configuration config) throws Exception {
			// Defining the descriptions of these states and info on how to serialize  these objects
			transactionState = getRuntimeContext().getState(new ValueStateDescriptor<>("Saved Transaction", Transaction.class));
			customerState = getRuntimeContext().getState(new ValueStateDescriptor<>("Saved Customer", Customer.class));

		}

		/* Logic: for flatMap2() and infer for flatMap1()
		 *   The order of arrival of transaction and customer is not something we have control over.
		 *   So, currently processing a fare with some ride_id, there could be the case where
		 *   ride with the same ride_id has not arrived yet. In this case we will have to wait
		 *   for the ride. So, that's where customerState comes into play, we can store the current
		 *   fare in customerState and have it processed later whenever that ride arrives.
		 *   Thus, the logic get ride from transactionState, if it's not present then add the current
		 *   fare to customerState. Else, if it is present remove it from the state and add it to
		 *   the output.
		 *   Same for flatMap1()
		 * */
		@Override
		public void flatMap1(Transaction transaction, Collector<CustomerTransaction> out) throws Exception {
			Customer customer = customerState.value();
			if (customer == null) {
				transactionState.update(transaction);
			}
			else {
				customerState.clear();
				out.collect(new CustomerTransaction(customer, transaction));
			}
		}

		@Override
		public void flatMap2(Customer customer, Collector<CustomerTransaction> out) throws Exception {
			Transaction transaction = transactionState.value();
			if (transaction == null) {
				customerState.update(customer);
			}
			else {
				transactionState.clear();
				out.collect(new CustomerTransaction(customer, transaction));
			}
		}
	}
}
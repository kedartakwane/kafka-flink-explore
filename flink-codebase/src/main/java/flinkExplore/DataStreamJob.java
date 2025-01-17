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
import objects.CustomerTransaction;
import objects.FraudTransaction;
import objects.Transaction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import serdes.CustomerJSONDeserializationSchema;
import serdes.CustomerTransactionJSONSerializationSchema;
import serdes.FraudTransactionJSONDeserializationSchema;
import serdes.TransactionJSONDeserializationSchema;

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
		final String KAFKA_TOPIC_FRAUD_TRANSACTIONS = "fraud_transactions_topic";
		final String KAFKA_TOPIC_OUTPUT = "customer_transactions_topic";
		final String FLINK_GROUP_ID = "FLINK_GROUP_ID";

		// Postgres Constants
		final String JDBC_URL = "jdbc:postgresql://localhost:5432/postgres";
		final String POSTGRES_USERNAME = "postgres";
		final String POSTGRES_PASSWORD = "postgres";

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

		KafkaSource<FraudTransaction> fraudTransactionSource = KafkaSource.<FraudTransaction>builder()
				.setBootstrapServers(KAFKA_BOOTRSTRAP_SERVER)
				.setTopics(KAFKA_TOPIC_FRAUD_TRANSACTIONS)
				.setGroupId(FLINK_GROUP_ID)
				.setStartingOffsets(OffsetsInitializer.earliest())
				.setValueOnlyDeserializer(new FraudTransactionJSONDeserializationSchema())
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
		DataStream<Transaction> transactionStream = env.fromSource(transactionSource, WatermarkStrategy.noWatermarks(), "Kafka Transactions Topic").keyBy(transaction -> transaction.getCustomerId());
		DataStream<Customer> customerStream = env.fromSource(customerSource, WatermarkStrategy.noWatermarks(), "Kafka Customer Topic").keyBy(customer -> customer.getCustomerId());
		DataStream<FraudTransaction> fraudTransactionStream = env.fromSource(fraudTransactionSource, WatermarkStrategy.noWatermarks(), "Kafka Fraud Transactions Topic").keyBy(transaction -> transaction.getReceiptId());

		// Window function to calculate totalAmount per customer per 10 seconds
		// Does not need a Watermark Strategy but for exploration added it as a new DataStream!
		DataStream<Transaction> transactionStream2 = env.fromSource(transactionSource,
				WatermarkStrategy.<Transaction>forMonotonousTimestamps()
						.withTimestampAssigner(
								((transaction, l) -> transaction.getReceiptDate().getTime())
						),
				"Kafka Transactions Topic");
		DataStream<Tuple2<String, Double>> ds = transactionStream2
				// Filter out objects that are null
				.filter(new FilterFunction<Transaction>() {
					@Override
					public boolean filter(Transaction transaction) throws Exception {
						return transaction != null;
					}
				})
				.map( new MapFunction<Transaction, Tuple2<String, Double>>() {
						  @Override
						  public Tuple2<String, Double> map(Transaction transaction) throws Exception {
								  String customerId = transaction.getCustomerId();
								  double totalAmount = transaction.getTotalAmount();
								  return new Tuple2<>(customerId, totalAmount);
						  }
					  }
				)
				// KeyBy customerId
				.keyBy(tuple2 -> tuple2.f0)
				.window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
				.reduce(new SumReduce());
		ds.print();

		// Join Transaction on customer to enrich data and put it in new DataStream
		DataStream<CustomerTransaction> customerTransactionDataStream = transactionStream.connect(customerStream).flatMap(new EnrichmentFunction());

		// Write to CustomerTransaction Topic
		customerTransactionDataStream.sinkTo(sink);

		// Write to Postgres Transactions Table
		JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(JDBC_URL)
				.withDriverName("org.postgresql.Driver")
				.withUsername(POSTGRES_USERNAME)
				.withPassword(POSTGRES_PASSWORD)
				.build();

		// Postgres Table Creation
		String sqlQuery = "CREATE TABLE IF NOT EXISTS Raw_Transactions (" +
				"receipt_id VARCHAR PRIMARY KEY," +
				"customer_id VARCHAR," +
				"product_id VARCHAR, " +
				"product_name VARCHAR, " +
				"product_price DOUBLE PRECISION, " +
				"product_quantity INTEGER, " +
				"total_amount DOUBLE PRECISION, " +
				"receipt_date TIMESTAMP, " +
				"payment_method VARCHAR" +
				")"
				;

		transactionStream.addSink(JdbcSink.sink(
				sqlQuery,
				(JdbcStatementBuilder<Transaction>) (preparedState, transaction) -> {

				},
				executionOptions,
				connectionOptions
		)).name("Created Raw_Transactions Table Sink!");

		String sqlQuery2 = "CREATE TABLE IF NOT EXISTS Customer (" +
				"customer_id VARCHAR PRIMARY KEY," +
				"name VARCHAR, " +
				"email VARCHAR, " +
				"birthdate VARCHAR" +
				")"
				;

		customerStream.addSink(JdbcSink.sink(
				sqlQuery2,
				(JdbcStatementBuilder<Customer>) (preparedState, transaction) -> {

				},
				executionOptions,
				connectionOptions
		)).name("Created Customer Table Sink!");

		String sqlQuery3 = "CREATE TABLE IF NOT EXISTS Fraud_Transactions (" +
				"receipt_id VARCHAR PRIMARY KEY," +
				"detected_ts TIMESTAMP" +
				")"
				;

		fraudTransactionStream.addSink(JdbcSink.sink(
				sqlQuery3,
				(JdbcStatementBuilder<FraudTransaction>) (preparedState, transaction) -> {

				},
				executionOptions,
				connectionOptions
		)).name("Created Fraud_Transactions Table Sink!");

//		String sqlQuery4 = "CREATE TABLE IF NOT EXISTS Customer_Transactions (" +
//				"receipt_id VARCHAR PRIMARY KEY," +
//				"customer_id VARCHAR," +
//				"product_id VARCHAR, " +
//				"product_name VARCHAR, " +
//				"product_price DOUBLE PRECISION, " +
//				"product_quantity INTEGER, " +
//				"total_amount DOUBLE PRECISION, " +
//				"receipt_date TIMESTAMP, " +
//				"payment_method VARCHAR, " +
//				"customer_name VARCHAR, " +
//				"customer_email VARCHAR, " +
//				"customer_birthdate VARCHAR" +
//				")"
//				;
//
//		customerTransactionDataStream.addSink(JdbcSink.sink(
//				sqlQuery4,
//				(JdbcStatementBuilder<CustomerTransaction>) (preparedState, customerTransaction) -> {
//
//				},
//				executionOptions,
//				connectionOptions
//		)).name("Create Customer_Transactions Table Sink!");

		// Inserting records to Postgres
		transactionStream.addSink(JdbcSink.sink(
				"INSERT INTO Raw_Transactions (receipt_id, customer_id, product_id, product_name, " +
						"product_price, product_quantity, total_amount, receipt_date, payment_method) " +
						"VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)" +
						"ON CONFLICT (receipt_id) DO UPDATE SET " +
						"receipt_id = EXCLUDED.receipt_id, " +
						"customer_id = EXCLUDED.customer_id, " +
						"product_id = EXCLUDED.product_id, " +
						"product_name = EXCLUDED.product_name, " +
						"product_price = EXCLUDED.product_price, " +
						"product_quantity = EXCLUDED.product_quantity, " +
						"total_amount = EXCLUDED.total_amount, " +
						"receipt_date = EXCLUDED.receipt_date, " +
						"payment_method = EXCLUDED.payment_method"
				,
				(JdbcStatementBuilder<Transaction>) (preparedStatement, transaction) -> {
					preparedStatement.setString(1, transaction.getReceiptId());
					preparedStatement.setString(2, transaction.getCustomerId());
					preparedStatement.setString(3, transaction.getProductId());
					preparedStatement.setString(4, transaction.getProductName());
					preparedStatement.setDouble(5, transaction.getProductPrice());
					preparedStatement.setInt(6, transaction.getProductQuantity());
					preparedStatement.setDouble(7, transaction.getTotalAmount());
					preparedStatement.setTimestamp(8, transaction.getReceiptDate());
					preparedStatement.setString(9, transaction.getPaymentMethod());
				},
				executionOptions,
				connectionOptions
		)).name("Inserted into Raw_Transactions Table Sink");

		customerStream.addSink(JdbcSink.sink(
				"INSERT INTO Customer (customer_id, name, email, birthdate) " +
						"VALUES (?, ?, ?, ?)" +
						"ON CONFLICT (customer_id) DO UPDATE SET " +
						"customer_id = EXCLUDED.customer_id, " +
						"name = EXCLUDED.name, " +
						"email = EXCLUDED.email, " +
						"birthdate = EXCLUDED.birthdate"
				,
				(JdbcStatementBuilder<Customer>) (preparedStatement, customer) -> {
					preparedStatement.setString(1, customer.getCustomerId());
					preparedStatement.setString(2, customer.getName());
					preparedStatement.setString(3, customer.getEmail());
					preparedStatement.setString(4, customer.getBirthdate());
				},
				executionOptions,
				connectionOptions
		)).name("Inserted into Customer Table Sink");

		fraudTransactionStream.addSink(JdbcSink.sink(
				"INSERT INTO Fraud_Transactions (receipt_id, detected_ts) " +
						"VALUES (?, ?)" +
						"ON CONFLICT (receipt_id) DO UPDATE SET " +
						"receipt_id = EXCLUDED.receipt_id, " +
						"detected_ts = EXCLUDED.detected_ts"
				,
				(JdbcStatementBuilder<FraudTransaction>) (preparedStatement, transaction) -> {
					preparedStatement.setString(1, transaction.getReceiptId());
					preparedStatement.setTimestamp(2, transaction.getDetectedTs());
				},
				executionOptions,
				connectionOptions
		)).name("Inserted into Fraud_Transactions Table Sink");

//		customerTransactionDataStream.addSink(JdbcSink.sink(
//				"INSERT INTO Customer_Transactions (receipt_id, customer_id, product_id, product_name, " +
//						"product_price, product_quantity, total_amount, receipt_date, payment_method, " +
//						"customer_name, customer_email, customer_birthdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)" +
//						"ON CONFLICT (receipt_id) DO UPDATE SET " +
//						"receipt_id = EXCLUDED.receipt_id, " +
//						"customer_id = EXCLUDED.customer_id, " +
//						"product_id = EXCLUDED.product_id, " +
//						"product_name = EXCLUDED.product_name, " +
//						"product_price = EXCLUDED.product_price, " +
//						"product_quantity = EXCLUDED.product_quantity, " +
//						"total_amount = EXCLUDED.total_amount, " +
//						"receipt_date = EXCLUDED.receipt_date, " +
//						"payment_method = EXCLUDED.payment_method, " +
//						"customer_name = EXCLUDED.customer_name, " +
//						"customer_email = EXCLUDED.customer_email, " +
//						"customer_birthdate = EXCLUDED.customer_birthdate",
//				(JdbcStatementBuilder<CustomerTransaction>) (preparedStatement, customerTransaction) -> {
//					preparedStatement.setString(1, customerTransaction.getReceiptId());
//					preparedStatement.setString(2, customerTransaction.getCustomerId());
//					preparedStatement.setString(3, customerTransaction.getProductId());
//					preparedStatement.setString(4, customerTransaction.getProductName());
//					preparedStatement.setDouble(5, customerTransaction.getProductPrice());
//					preparedStatement.setInt(6, customerTransaction.getProductQuantity());
//					preparedStatement.setDouble(7, customerTransaction.getTotalAmount());
//					preparedStatement.setTimestamp(8, customerTransaction.getReceiptDate());
//					preparedStatement.setString(9, customerTransaction.getPaymentMethod());
//					preparedStatement.setString(10, customerTransaction.getCustomerName());
//					preparedStatement.setString(11, customerTransaction.getCustomerEmail());
//					preparedStatement.setString(12, customerTransaction.getCustomerBirthdate());
//				},
//				executionOptions,
//				connectionOptions
//		)).name("Insert into Customer_Transactions Table Sink");

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

	private static class SumReduce
			implements ReduceFunction<Tuple2<String, Double>> {

		@Override
		public Tuple2<String, Double> reduce(Tuple2<String, Double> t1, Tuple2<String, Double> t2) throws Exception {
			return t1 != null && t2 != null ? new Tuple2<>(t1.f0, t1.f1 + t2.f1): new Tuple2<>("test", 0D);
		}
	}

}
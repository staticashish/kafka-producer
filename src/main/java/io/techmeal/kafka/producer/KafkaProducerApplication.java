package io.techmeal.kafka.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Kafka producer application !!
 *
 */
public class KafkaProducerApplication {
	private static final String KAFKA_PRODUCER_TOPIC = "producer.topic";
	private static final String KAFKA_SERVER = "kafka.server";
	private static final String IS_ASYNC = "isAsync";
	private static String topic;
	private static String kafkaServer;
	private static boolean isAsync;
	
    public static void main( String[] args ) throws IOException {
    	setup();
    	KafkaProducerConfig config = new KafkaProducerConfig();
    	config.setBootstrapServer(kafkaServer);
    	config.setTopics(topic);
    	config.setIsAsync(isAsync);
        Producer producer = new Producer(config);
        producer.init();
    }

	private static void setup() throws IOException {
		InputStream is = KafkaProducerApplication.class.getClassLoader().getResourceAsStream("kafka.properties");
    	Properties props = new Properties();
    	props.load(is);
    	kafkaServer = (String) props.get(KAFKA_SERVER);
    	topic = (String) props.get(KAFKA_PRODUCER_TOPIC);
    	isAsync = Boolean.valueOf((String) props.get(IS_ASYNC));
	}
}

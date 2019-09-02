package io.techmeal.kafka.producer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class Producer {

	private KafkaProducerConfig config;
	
	public Producer(KafkaProducerConfig config) {
		this.config = config;
	}

	public void init() {
		Properties properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getBootstrapServer());
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		
		KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(properties);
		int counter = 0;
		while(true) {
			String value = "message - "+counter;
			ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(config.getTopics(), value);
			
			if(config.getIsAsync()) {
				kafkaProducer.send(producerRecord, (r,e) -> {
					if(e == null) {
						printMetadata(r);
					} else {
						e.printStackTrace();
					}
				});
			} else {
				try {
					RecordMetadata recordMetadata = kafkaProducer.send(producerRecord).get();
					printMetadata(recordMetadata);
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
			try {
				Thread.sleep(1000);
				++counter;
				if(counter > 20) {
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private void printMetadata(RecordMetadata recordMetadata) {
		System.out.println("Recieved Metadata: \n" +
				"Topic : "+recordMetadata.topic() +" \n" +
				"Partition : "+recordMetadata.partition() +" \n" +
				"Offset : "+recordMetadata.offset() +" \n" +
				"Timestamp : "+recordMetadata.timestamp());
	}
}

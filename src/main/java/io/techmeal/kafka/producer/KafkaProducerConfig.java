package io.techmeal.kafka.producer;

public class KafkaProducerConfig {

	String bootstrapServer;
	String topics;
	Boolean isAsync;

	public String getBootstrapServer() {
		return bootstrapServer;
	}
	
	public void setBootstrapServer(String bootstrapServer) {
		this.bootstrapServer = bootstrapServer;
	}

	public String getTopics() {
		return topics;
	}

	public void setTopics(String topics) {
		this.topics = topics;
	}

	public Boolean getIsAsync() {
		return isAsync;
	}

	public void setIsAsync(Boolean isAsync) {
		this.isAsync = isAsync;
	}
	
}

package com.spacentime.mb.common;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Callback;

import com.spacentime.mb.core.contracts.IConsumerCallbackRecord;

public class ServiceBusPayload {
	
	// Producer and Consumer related common parameters.
	private int type;
	private Properties properties = null;
	
	// Producer related parameter.
	private KafkaEvent event;
	private Callback producerCallback;
	
	//Consumer related parameter.
	private String topicName;
	private IConsumerCallbackRecord consumerCallbackRecord;
	private BlockingQueue<ConsumerRecord<String, String>> resultStore;
	
	public int getType() {
		return type;
	}
	
	public void setType(int type) {
		this.type = type;
	}
	
	public Properties getProperties() {
		return properties;
	}
	
	public void setProperties(Properties properties) {
		this.properties = properties;
	}
	
	public KafkaEvent getEvent() {
		return event;
	}
	
	public void setEvent(KafkaEvent event) {
		this.event = event;
	}
	
	public String getTopicName() {
		return topicName;
	}
	
	public void setTopicName(String topicName) {
		this.topicName = topicName;
	}

	public BlockingQueue<ConsumerRecord<String, String>> getResultStore() {
		return resultStore;
	}

	public void setResultStore(BlockingQueue<ConsumerRecord<String, String>> resultStore) {
		this.resultStore = resultStore;
	}

	public Callback getProducerCallback() {
		return producerCallback;
	}

	public void setProducerCallback(Callback producerCallback) {
		this.producerCallback = producerCallback;
	}

	public IConsumerCallbackRecord getConsumerCallbackRecord() {
		return consumerCallbackRecord;
	}

	public void setConsumerCallbackRecord(IConsumerCallbackRecord consumerCallbackRecord) {
		this.consumerCallbackRecord = consumerCallbackRecord;
	}
}

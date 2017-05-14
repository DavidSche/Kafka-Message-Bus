package com.spacentime.mb.core;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;

import com.spacentime.mb.common.ApplicationProperty;
import com.spacentime.mb.common.KafkaEvent;
import com.spacentime.mb.common.ServiceBusConstants;
import com.spacentime.mb.common.ServiceBusException;
import com.spacentime.mb.core.contracts.IConsumerCallbackRecord;
import com.spacentime.mb.core.contracts.IServiceBus;
import com.spacentime.mb.core.contracts.IServiceBusDelegator;
import com.spacentime.mb.core.contracts.ServiceBusDelegatorFactory;

/**
 * Facade class to send/receive kafka message.
 * 
 * 1) KafkaServiceBus is builder/facade class which can Producer and Consumer with different configuration.
 * 2) Events of type KafkaEvent as of now. (log files,messages,streams we can see in future)
 * 3) Should support creating threads of Producer or Consumer for sending messages.
 * 4) Producer and Consumer should support topic partitioning and grouping.
 * 5) Consumer should support polling vs subscribe model to receive the event .
 * 6) Consumer shall support Event processors.
 * 7) Shall support Avro (Schema oriented) serializer/de-serializer while exchanging the events.
 * 8) Encoder.
 * 
 * @author Subrata Saha
 *
 */
public class KafkaServiceBus implements IServiceBus {
	
	private IServiceBusDelegator delegator = null;
	
	public KafkaServiceBus(int type){
		delegator = new ServiceBusDelegatorFactory().getDelaegator(type);
		if(delegator == null){
			throw new ServiceBusException(ServiceBusException.DELEGATOR_TYPE_MISSING);
		}
		
		delegator.getPayload().setType(type);
	}
	
	public IServiceBus attachConfiguration(Properties properties) {
		if(properties != null){
			delegator.getPayload().setProperties(properties);
		}
		return this;
	}

	//************************************************ Start Producer API implementation ************************************************
	@Override
	public IServiceBus warmupProducer(KafkaEvent event,Callback callback) {
		// TODO - The topic should be derived from KafkaEvent.
		String topicName = ServiceBusConstants.TOPIC_FOR_EVENT;
		if(ApplicationProperty.getInstance().getPropertyValue(event.getUserIDForEvent()) != null){
			topicName = ApplicationProperty.getInstance().getPropertyValue(event.getUserIDForEvent());
		}
		
		delegator.getPayload().setEvent(event);
		delegator.getPayload().setProducerCallback(callback);
		delegator.getPayload().setTopicName(topicName);
		
		return this;
	}
	
	//************************************************ End Producer API implementation ************************************************

	//************************************************ Start Consumer API implementation **********************************************

	@Override
	public IServiceBus warmupConsumerForTopic(String topicName,IConsumerCallbackRecord record) {
		delegator.getPayload().setTopicName(topicName);
		delegator.getPayload().setConsumerCallbackRecord(record);
		
		return this;
	}
	
	//************************************************ End Consumer API implementation ************************************************

	@Override
	public void start() {
		if(delegator.isValid()){
			delegator.startProcessing();
		}else{
			throw new ServiceBusException(ServiceBusException.SERVICE_BUS_CONFIGURATION_INVALID);
		}
	}
	
	@Override
	public void close() {
		if(delegator != null){
			delegator.stopProcessing();
		}
	}
}

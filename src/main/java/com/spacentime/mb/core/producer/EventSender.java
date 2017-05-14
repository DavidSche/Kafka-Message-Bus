package com.spacentime.mb.core.producer;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spacentime.mb.common.KafkaEvent;
import com.spacentime.mb.common.ServiceBusPayload;
import com.spacentime.mb.core.contracts.IServiceBusDelegator;
/**
 * The actual Producer work is done here.
 * 
 * @author Subrata Saha
 *
 */
public class EventSender implements IServiceBusDelegator {

	private static final Logger log = LoggerFactory.getLogger(EventSender.class);

	private ServiceBusPayload payload;
	private KafkaProducer<String,KafkaEvent> producer = null;
	
	
	public EventSender(){
		this.payload = new ServiceBusPayload();
	}
	
	@Override
	public void startProcessing() {
		ProducerRecord<String, KafkaEvent> recordToBeSent = new ProducerRecord<>(payload.getTopicName(),payload.getEvent());
		
		producer = new KafkaProducer<>(payload.getProperties());
		Future<RecordMetadata> f = producer.send(recordToBeSent,payload.getProducerCallback()); 
		
		if(f != null){
			try {
				RecordMetadata md = f.get();
				//System.out.println("Send partition is ::"+md.partition());
			} catch (InterruptedException | ExecutionException e) {
				//
			} 
		}
		System.out.println("Message send >>>"+payload.getEvent().getEventValue());
		
		if (log.isDebugEnabled()) {
			log.debug("[SpaceNTime] :: EventSender -> startProcessing ( Message sent to topc ::"
					+ payload.getTopicName() + " with key ::" + payload.getEvent().getUserIDForEvent() + " with value ::"
					+ payload.getEvent().getEventValue());
		}
	}

	@Override
	public ServiceBusPayload getPayload() {
		return payload;
	}

	@Override
	public void stopProcessing() {
		if(producer != null){
			producer.close();
		}
	}
	
	@Override
	public boolean isValid() {
		return (payload != null && payload.getTopicName() != null && payload.getEvent() != null
				&& payload.getProperties() != null);
	}
}

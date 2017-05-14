package com.spacentime.mb.client;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;

import com.spacentime.mb.common.KafkaEvent;
import com.spacentime.mb.common.KafkaEventType;
import com.spacentime.mb.core.KafkaServiceBus;
import com.spacentime.mb.core.contracts.IServiceBus;

public class ServiceBusProducerClient {
	
	public static void main(String[] args) {
		// Create and Event 
		KafkaEvent event = new KafkaEvent(KafkaEventType.GENERIC, "SUBRATA AND KafkaEventKyroSerializer 4...");
		
		// Create configuration
		Properties properties = getProducerConfigProperty();
		
		// we are using the service bus for sending message only.
		IServiceBus bus = (IServiceBus) new KafkaServiceBus(IServiceBus.SERVICE_BUS_FOR_SENDING_RECORDS)
				.attachConfiguration(properties)
				.warmupProducer(event,new Callback(){

					@Override
					public void onCompletion(RecordMetadata metaData, Exception exception) {
						if(exception == null){
							System.out.println("Topic got send to:: "+metaData.topic());
							System.out.println("Partition :: "+metaData.partition());
							System.out.println("Offset :: "+metaData.offset());
						}else{
							// TODO check the exceptions to find out the issues.
							System.out.println("Exception ....");
						}
					}
					
				});
		
		bus.start();
	}
		

	private static Properties getProducerConfigProperty() {
		Properties properties = new Properties();
        //properties.put("request.required.acks", "1");
        
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("partitioner.class", "com.spacentime.mb.core.producer.KafkaEventPartitioner");
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "com.spacentime.mb.common.KafkaEventKyroSerializer");
        
        return properties;
	}
}

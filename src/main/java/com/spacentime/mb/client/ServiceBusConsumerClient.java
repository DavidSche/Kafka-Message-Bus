package com.spacentime.mb.client;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.spacentime.mb.common.KafkaEvent;
import com.spacentime.mb.common.ServiceBusConstants;
import com.spacentime.mb.core.KafkaServiceBus;
import com.spacentime.mb.core.contracts.IConsumerCallbackRecord;
import com.spacentime.mb.core.contracts.IServiceBus;

/**
 * Client example for receiving messages...
 * @author Subrata Saha
 *
 */
public class ServiceBusConsumerClient {
	
	public static void main(String[] args) {
		// result listener.
		ResultListener listener = new ResultListener();
		
		//Async way
		IServiceBus bus = (IServiceBus) new KafkaServiceBus(IServiceBus.SERVICE_BUS_FOR_RECEIVING_RECORDS_ASYNC)
				.attachConfiguration(getConsumerConfigProperty())
				.warmupConsumerForTopic(ServiceBusConstants.TOPIC_FOR_EVENT,listener);
		bus.start();
		
		if(!listener.isDataFetchingFinished()){
			try {
				System.out.println("**** Going to wait for 10000L ...");
				Thread.sleep(100000L);
			} catch (InterruptedException e) {
				// nothing as of now.
			}
		}
		
		System.out.println("***************** Subrata  ServiceBusConsumerClient.main(Closing it down !!)");
		bus.close();
	}
	
	private static Properties getConsumerConfigProperty() {
		Properties props = new Properties();

		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "event.type.generic");
		props.put("enable.auto.commit", "false");
		//props.put("auto.commit.interval.ms", "1000");
		props.put("fetch.max.wait.ms", "1");
		
		// setting to ensure that the handler has enough time to finish processing messages
		// The second option is to do message processing in a separate thread,
		// It may even exacerbate the problem if the poll loop is stuck blocking on a call to offer() 
		// while the background thread is handling an even larger batch of messages. The Java API offers a pause() method to help 
		props.put("session.timeout.ms", "30000");
		//props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		//props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "com.spacentime.mb.common.KafkaEventKyroSerializer");
	  

		// https://github.com/nielsutrecht/kafka-serializer-example/blob/master/src/main/java/com/nibado/example/kafka/serialization/StringReadingSerializer.java
		return props;
	}
}


 class ResultListener implements IConsumerCallbackRecord {
	
	private BlockingQueue<ConsumerRecord<String, KafkaEvent>> recordStore = null;
	
	private boolean dataFetchingStarted = true;
	
	public BlockingQueue<ConsumerRecord<String, KafkaEvent>> getResult(){
		return recordStore;
	}
	
	public boolean isDataFetchingFinished(){
		return !dataFetchingStarted;
	}
	
	@Override
	public void startMessageListening(BlockingQueue<ConsumerRecord<String, KafkaEvent>> recordStore) {
		this.recordStore = recordStore;
		new Thread(new DataProcessorThread()).start();
	}
	
	class DataProcessorThread implements Runnable {

		@Override
		public void run() {
			while(dataFetchingStarted){
				try {
					ConsumerRecord<String, KafkaEvent> cr = recordStore.take();
					System.out.println("***************** Receiving message ::"+cr.value().getEventValue());
					if(cr.value().getEventValue().equals(ServiceBusConstants.POISON)){
						dataFetchingStarted = false;
					}
				} catch (InterruptedException e) {
					// no cancellation policy applies here.
				}
			}
		}
	} 
	
	
	
}

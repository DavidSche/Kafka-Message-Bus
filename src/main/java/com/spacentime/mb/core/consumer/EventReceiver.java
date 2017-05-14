package com.spacentime.mb.core.consumer;

import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spacentime.mb.common.KafkaEvent;
import com.spacentime.mb.common.ServiceBusConstants;
import com.spacentime.mb.common.ServiceBusPayload;
import com.spacentime.mb.core.contracts.IServiceBusDelegator;
/**
 * The Actual Kafka Consumer work is done here.
 * 
 * @author Subrata Saha
 *
 */
public class EventReceiver implements IServiceBusDelegator{

	private static final Logger log = LoggerFactory.getLogger(EventReceiver.class);
	
	private ServiceBusPayload payload;
	private ExecutorService executor = null;
	private List<KafkaConsumerWorker> workers = null;
	private BlockingQueue<ConsumerRecord<String, KafkaEvent>> recordStore = new LinkedBlockingQueue<>();
	
	public EventReceiver(){
		this.payload = new ServiceBusPayload();
	}
	
	@Override
	public void startProcessing() {
		KafkaConsumer<String,KafkaEvent> consumer = new KafkaConsumer<>(payload.getProperties());
		List<PartitionInfo> partionList = null;
		
		if(consumer != null && payload.getTopicName() != null){
			partionList = consumer.partitionsFor(payload.getTopicName());
			consumer.close();
		}
		
		if(partionList != null){
			startConsumers(partionList);
			//Deque<Future<ConsumerRecords<String, KafkaEvent>>> futureList = startConsumers(partionList);
			//handleResult(futureList);
		}
	}

	//private Deque<Future<ConsumerRecords<String, KafkaEvent>>> startConsumers(List<PartitionInfo> partionList) {
	private void startConsumers(List<PartitionInfo> partionList) {
		// creating more consumer than partition size has no benefit.
		int consumer_size = partionList.size();
		if (log.isDebugEnabled()) {
		 log.debug("[SpaceNTime] :: EventReceiver -> startConsumers() -> PartitionInfo size "+consumer_size);
	    }
		
		//Deque<Future<ConsumerRecords<String, KafkaEvent>>> futureList = new LinkedBlockingDeque<>(consumer_size);
		executor = Executors.newFixedThreadPool(consumer_size);
		workers = new ArrayList<>(consumer_size);
		
		//Ask client to prepare for recording the data
		payload.getConsumerCallbackRecord().startMessageListening(recordStore);

		
		for(int i=0;i<partionList.size();i++){
			KafkaConsumerWorker worker = new KafkaConsumerWorker(payload,recordStore);
			executor.submit(worker);
			//futureList.add(executor.submit(worker));
			workers.add(worker);
			if (log.isDebugEnabled()) {
				log.debug("[SpaceNTime] :: EventReceiver -> startProcessing() -> created worker thread "+worker);
			}
		}
		
		//return futureList;
	}

	private void handleResult(Deque<Future<ConsumerRecords<String, KafkaEvent>>> futureList) {
		
		for(Future<ConsumerRecords<String, KafkaEvent>> f : futureList){
			try {
				// Blocking call to get the result from threads.
				ConsumerRecords<String, KafkaEvent> records = f.get();
				records.forEach(record -> recordStore.offer(record)); 
				
				//send a poision to end the result loop.
				recordStore.offer(new ConsumerRecord(ServiceBusConstants.TOPIC_FOR_EVENT, 0, 0, ServiceBusConstants.POISON,
						new KafkaEvent(ServiceBusConstants.POISON, ServiceBusConstants.POISON)));
		
				//send notification
				payload.getConsumerCallbackRecord().startMessageListening(recordStore);

			} catch (InterruptedException | ExecutionException e) {
				// no cancellation policy applies here.
			}
		}
		
		executor.shutdownNow();
	}

	@Override
	public ServiceBusPayload getPayload() {
		return payload;
	}

	@Override
	public void stopProcessing() {
		
		// Shutdown the worker threads.
		if(workers != null){
			workers.forEach(worker -> {
				try {
					worker.shutDown();
				} catch (InterruptedException e) {
					// no cancellation policy applies here.
				}
			});
		}
		if (log.isDebugEnabled()) {
			log.debug("[SpaceNTime] :: EventReceiver -> stopProcessing() -> Stopped all worker thread..");
		}
		
		// Shut down the executor..
		if (executor != null) executor.shutdown();
	    try {
	        if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
	            System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
	        }
	    } catch (InterruptedException e) {
	        System.out.println("Interrupted during shutdown, exiting uncleanly");
	    }
	    if (log.isDebugEnabled()) {
			log.debug("[SpaceNTime] :: EventReceiver -> stopProcessing() -> Stopped all thread pool..");
		}
	    
	    //finally bring it down.
	    executor.shutdownNow();
	    
	    //finally post poison to shut down message listening by client.
		recordStore.offer(new ConsumerRecord(ServiceBusConstants.TOPIC_FOR_EVENT, 0, 0, ServiceBusConstants.POISON,
				new KafkaEvent(ServiceBusConstants.POISON, ServiceBusConstants.POISON)));

	}

	@Override
	public boolean isValid() {
		return (payload != null && payload.getTopicName() != null 
				&& payload.getProperties() != null);
	}

}

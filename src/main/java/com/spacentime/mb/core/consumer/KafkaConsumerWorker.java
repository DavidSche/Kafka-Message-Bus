package com.spacentime.mb.core.consumer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.CommitFailedException;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.spacentime.mb.common.KafkaEvent;
import com.spacentime.mb.common.ServiceBusPayload;
/**
 * Parallel Kafka Consumer thread which will read messages from all partition.
 * @author Subrata Saha
 *
 */
public class KafkaConsumerWorker implements Callable<ConsumerRecords<String, KafkaEvent>> {

	private static final Logger log = LoggerFactory.getLogger(KafkaConsumerWorker.class);
	
	private KafkaConsumer<String, KafkaEvent> consumer;
	private ServiceBusPayload payload;
	private BlockingQueue<ConsumerRecord<String, KafkaEvent>> resultStore = null;

	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private final CountDownLatch shutdownLatch = new CountDownLatch(1);

	public KafkaConsumerWorker(ServiceBusPayload payload,BlockingQueue<ConsumerRecord<String, KafkaEvent>> recordStore) {
		this.payload = payload;
		this.resultStore = recordStore;
		this.consumer = new KafkaConsumer<>(payload.getProperties()); 
	}

	@Override
	public ConsumerRecords<String, KafkaEvent> call() {
		ConsumerRecords<String, KafkaEvent> records = null;
		try {
			//consumer.assign(Arrays.asList(new TopicPartition(partionList.get(i).topic(),partionList.get(i).partition())));
			consumer.subscribe(Arrays.asList(payload.getTopicName()), new ConsumerRebalanceListener() {
				@Override
				public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
					synchronizedCommit();
					if (log.isDebugEnabled()) {
						log.debug("[SpaceNTime] :: KafkaConsumerWorker -> consumer.subscribe->onPartitionsRevoked() do synchronizedCommit !!");
					}
				}

				@Override
				public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
					//TODO - always seeking from beginning , need to change..
					consumer.seekToBeginning(partitions);
				}
			});

			while (!shutdown.get()) {
				records = consumer.poll(Integer.MAX_VALUE);
				records.forEach(record -> resultStore.offer(record)); 
				
				if (log.isDebugEnabled()) {
					log.debug("[SpaceNTime] :: KafkaConsumerWorker -> run() -> Message Received .. processing..");
				}
				
				// committing the message to broker
				/*
				 * If you need more reliability, synchronous commits are there
				 * for you, and you can still scale up by increasing the number
				 * of topic partitions and the number of consumers in the group.
				 * But if you just want to maximize throughput and you’re
				 * willing to accept some increase in the number of duplicates,
				 * then asynchronous commits may be a good option.
				 */

				// SYNC - if(doCommitSync()){records.forEach(record ->
				// handler.notifyAsyncRecord(record));}
				// Pro - dont need to handle commit fail as it will retry until
				// success :
				// Con - block the thread until all commit success.

				// ASYN - the following.
				// Pro - throughput is good as no blocking.
				// Con - retry and re ordering logic should be implemented
				// correctly.
				consumer.commitAsync(new OffsetCommitCallback() {
					public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
						
						if (exception != null) {
							// from topic partition.topic() ->
							// partition.partition() ->
							// offsetAndMetadata.offset() , do something !!
							// TODO - Client should handle the message in
							// separate thread and take care of message,
							// ideally logging is fine here but We will revisit.
							// payload.getHandler().notifyError(code);
						}
					}
				});
				
				//shutdown.set(true);  //Dont stop listening.
			}
			
			if (log.isDebugEnabled()) {
				log.debug("[SpaceNTime] :: KafkaConsumerWorker -> run ( Consumer thread getting stopped !!)");
			}

		} catch (WakeupException e) {
			// ignore, we're closing
			if (log.isDebugEnabled()) {
				log.debug("[SpaceNTime] :: KafkaConsumerWorker -> WakeupException ( We can Ignore it )"+e.getMessage());
			}
		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("[SpaceNTime] :: KafkaConsumerWorker -> Exception ( We can Ignore it )"+e.getMessage());
			}
		} finally {
			try {
				synchronizedCommit();
			} finally {
				consumer.close();
				shutdownLatch.countDown();
				if (log.isDebugEnabled()) {
					log.debug("[SpaceNTime] :: KafkaConsumerWorker -> closing all resourse( We can Ignore it )");
				}
			}
		}
		return records;
	}

	@SuppressWarnings("unused")
	public void shutDown() throws InterruptedException {
		if (log.isDebugEnabled()) {
			log.debug("[SpaceNTime] :: KafkaConsumerWorker -> shutDown requested !!");
		}
		consumer.wakeup(); // if long polling is happening then it will used to stop it.
		shutdown.set(true);
		shutdownLatch.await();
	}


	private boolean synchronizedCommit() {
		try {
			consumer.commitSync();
			if (log.isDebugEnabled()) {
				log.debug("[SpaceNTime] :: KafkaConsumerWorker -> synchronizedCommit requested !!");
			}
			return true;
		} catch (CommitFailedException e) {
			// the commit failed with an unrecoverable error.
			consumer.close();
			if (log.isDebugEnabled()) {
				log.debug("[SpaceNTime] :: KafkaConsumerWorker -> CommitFailedException falied , so consumer.close() happened!!");
			}
			return false;
		}
	}

}

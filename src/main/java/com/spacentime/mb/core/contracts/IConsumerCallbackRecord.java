package com.spacentime.mb.core.contracts;

import java.util.concurrent.BlockingQueue;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import com.spacentime.mb.common.KafkaEvent;

/**
 * Consumer related configuration API for aggregator.
 * @author Subrata Saha
 *
 */
public interface IConsumerCallbackRecord {
	
	// start listening the queue on getting this notification.
	public void startMessageListening(BlockingQueue<ConsumerRecord<String, KafkaEvent>> recordStore);
   
}

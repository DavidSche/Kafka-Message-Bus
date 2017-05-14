package com.spacentime.mb.core.contracts;

import org.apache.kafka.clients.producer.Callback;

import com.spacentime.mb.common.KafkaEvent;
/**
 * 1) The producer sends data directly to the broker that is the leader for the partition.
 * 2) Kafka producer will attempt to accumulate data in memory and to send out larger batches 
 *    in a single request.
 *    
 * @author Subrata Saha.
 *
 */
public interface IProducerConfiguration {
	
	   // Producer side API	
	   public IProducerConfiguration warmupProducer(KafkaEvent event,Callback callback);
}

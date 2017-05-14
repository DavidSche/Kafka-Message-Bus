package com.spacentime.mb.core.contracts;

/**
 * 1) The consumer specifies its offset in the log with each request and receives back a 
 *    chunk of log beginning from that position.
 * 2) Pull vs Consistant listening via listener.
 * 3) sent not consumed vs consume (i.e ack)
 * 
 * @author Subrata Saha
 *
 */
public interface IConsumerConfiguration {
	   // Consumer site API
	   
	   
	   // to receive Records from a single Topics for all available partitions.
	   public IConsumerConfiguration warmupConsumerForTopic(String topicName,IConsumerCallbackRecord callbackRecord);
	   
}

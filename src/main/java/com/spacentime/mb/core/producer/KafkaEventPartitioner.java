package com.spacentime.mb.core.producer;

import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Partitioner class for KafkaEvent type.
 * 1) It should create the Partition key based on KafkaEvent key type which will be 
 *    used to partition the topic.
 *    
 * @author Subrata Saha
 *
 */
public class KafkaEventPartitioner implements Partitioner{

	private static final Logger log = LoggerFactory.getLogger(KafkaEventPartitioner.class);
	
	@Override
	public void configure(Map<String, ?> arg0) {
		// same properties file as we created during construction of producer/consumer
	}

	@Override
	public void close() {
		// called when partition is closed.
	}

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		//return partition(key,10);
		List<PartitionInfo> partitions = cluster.partitionsForTopic(topic);
        int numPartitions = partitions.size();
        
        // convert it to some positive number and find the hash out of available partitions.
        int partitionId = (Utils.murmur2(valueBytes) & 0x7fffffff) % numPartitions;
		if (log.isDebugEnabled()) {
			log.debug("[SpaceNTime] :: KafkaEventPartitioner -> partition ( topic ::" + topic + " key ::" + key
					+ " partitionId created ::" + partitionId);
		}
        return partitionId;
	}
	
}

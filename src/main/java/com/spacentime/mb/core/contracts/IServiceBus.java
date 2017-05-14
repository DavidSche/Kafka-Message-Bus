package com.spacentime.mb.core.contracts;

import java.util.Properties;

/**
 * Let this interface take care only Kafka implementation.
 * 
 * @author Subrata Saha
 *
 */
public interface IServiceBus extends IProducerConfiguration,IConsumerConfiguration{
   
   // SEND THE RECORDS 	
   public static final int SERVICE_BUS_FOR_SENDING_RECORDS = 1;
   
   // POLL ALL THE ACCUMULATED RECORDS AT ONCE.
   public static final int SERVICE_BUS_FOR_POLLING_RECORDS_SYNC = 2;
   
   // KEEP LISTENING FOR RECORD AND RECEIVE IT AS SOON AS THEY ARRIVE
   public static final int SERVICE_BUS_FOR_RECEIVING_RECORDS_ASYNC = 3;
   
   
   public IServiceBus attachConfiguration(Properties properties);
   
   public void start();
   
   public void close();
}

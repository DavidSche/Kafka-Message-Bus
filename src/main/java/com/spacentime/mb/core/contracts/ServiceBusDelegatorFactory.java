package com.spacentime.mb.core.contracts;

import com.spacentime.mb.core.consumer.EventReceiver;
import com.spacentime.mb.core.producer.EventSender;

/**
 * Delegator factory to Producer/Consumer delegator.
 * @author Subrata Saha
 *
 */
public class ServiceBusDelegatorFactory {
	
	public IServiceBusDelegator getDelaegator(int type) {
		IServiceBusDelegator delegator = null;
		switch (type) {
		case IServiceBus.SERVICE_BUS_FOR_SENDING_RECORDS:
			delegator = new EventSender();
			break;
		case IServiceBus.SERVICE_BUS_FOR_RECEIVING_RECORDS_ASYNC:
			delegator = new EventReceiver();
			break;
		}
		return delegator;
	}
	
}

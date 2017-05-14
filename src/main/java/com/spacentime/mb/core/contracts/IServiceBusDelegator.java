package com.spacentime.mb.core.contracts;

import com.spacentime.mb.common.ServiceBusPayload;

/**
 * Delegator interface through which Producer and Consumer will be done in separate class.
 * @author Subrata Saha
 *
 */
public interface IServiceBusDelegator {
	
    public void startProcessing();
    
    public ServiceBusPayload getPayload();
    
    public void stopProcessing();
    
    public boolean isValid();
}

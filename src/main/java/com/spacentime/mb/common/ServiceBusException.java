package com.spacentime.mb.common;
/**
 * Exception class for Service Bus.
 * @author Subrata Saha
 *
 */
public class ServiceBusException extends RuntimeException {
	
	public static final int DELEGATOR_TYPE_MISSING = 1;
	public static final int SERVICE_BUS_CONFIGURATION_INVALID = 2;
	
	private static final long serialVersionUID = 7111470871437057592L;
	
	private int code;
	
	public ServiceBusException(int code){
		this.code = code;
	}
	
	public ServiceBusException(int code,String msg){
		super(msg);
		this.code = code;
	}

	public int getCode() {
		return code;
	}
}

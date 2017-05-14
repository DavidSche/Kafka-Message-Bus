package com.spacentime.mb.common;

import java.io.Serializable;

/**
 * The event payload to send/receive to and from kafka brokers.
 * @author Subrata Saha
 *
 */
public class KafkaEvent implements Serializable {
	
	private static final long serialVersionUID = -6173744509492239058L;

	// from where this event is originated (e.g Mobile client,Web etc)
	private int source;
	
	// the event key , this will determine the partition key in the topic.
	private String userIDForEvent;
	
	// actual payload if any , if it is merely a trigger to do some operation 
	// then this field may be empty.
	private String eventValue;
	
	public KafkaEvent() {
		super();
	}
	public KafkaEvent(String eventKey, String eventValue) {
		this.userIDForEvent = eventKey;
		this.eventValue = eventValue;
	}

	public KafkaEvent(String eventKey, int source, String eventValue) {
		this.userIDForEvent = eventKey;
		this.source = source;
		this.eventValue = eventValue;
	}

	public String getUserIDForEvent() {
		return userIDForEvent;
	}

	public int getSource() {
		return source;
	}

	public String getEventValue() {
		return eventValue;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((userIDForEvent == null) ? 0 : userIDForEvent.hashCode());
		result = prime * result + ((eventValue == null) ? 0 : eventValue.hashCode());
		result = prime * result + source;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		KafkaEvent other = (KafkaEvent) obj;
		if (userIDForEvent == null) {
			if (other.userIDForEvent != null)
				return false;
		} else if (!userIDForEvent.equals(other.userIDForEvent))
			return false;
		if (eventValue == null) {
			if (other.eventValue != null)
				return false;
		} else if (!eventValue.equals(other.eventValue))
			return false;
		if (source != other.source)
			return false;
		return true;
	}

	@Override
	public String toString() {
		return "KafkaEvent [eventKey=" + userIDForEvent + ", source=" + source + ", eventValue=" + eventValue + "]";
	}
}

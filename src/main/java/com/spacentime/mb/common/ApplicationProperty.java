package com.spacentime.mb.common;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.common.utils.Utils;
/**
 * Static class to read the property file for Kafka properties and get when required.
 * 
 * @author Subrata Saha
 *
 */
public class ApplicationProperty {

	private String propertyFileName = "mb.properties";
	
	private static Properties applicationProps;

	private static class ApplicationPropertyHolder {
		private static ApplicationProperty applicationProperty = new ApplicationProperty();
	}

	private ApplicationProperty() {
		loadApplicationProperties();
	}

	private void loadApplicationProperties() {
		ClassLoader classLoader = getClass().getClassLoader();
		String propertyFilePath = (classLoader.getResource(propertyFileName)).toString();
		try {
			applicationProps = Utils.loadProps(propertyFilePath);
		} catch (IOException e) {
			//
		}
	}
	
	public static ApplicationProperty getInstance(){
		return ApplicationPropertyHolder.applicationProperty;
	}
	
	public String getPropertyValue(String key){
		String value = null;
		if(applicationProps != null){
			value = applicationProps.getProperty(key);
		}
		return value;
	}
	
}

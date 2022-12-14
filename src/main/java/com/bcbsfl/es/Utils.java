package com.bcbsfl.es;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Properties;

public class Utils {
    static protected Properties appProps = null;
    static public void init() {
        try (InputStream input = new FileInputStream("src/main/resources/application.properties")) {
            appProps = new Properties();	            
            appProps.load(input);
            String key = null, propValue = null, envValue = null, message = null;
            System.out.println("***************************** Application properties *****************************");
            for(Iterator<Object> iter = appProps.keySet().iterator(); iter.hasNext();) {
            	key = (String) iter.next();
            	envValue = System.getenv(key.replace(".", "_"));
            	propValue = appProps.getProperty(key);
            	message = envValue == null ? "Using application.properties value" : "Using Environment value";
            	System.out.println(key + ": " + (envValue == null ? propValue : envValue) + " (" + message + ")");
            }
            System.out.println("*************************** End Application properties ***************************");
        } catch(FileNotFoundException e) {
        	System.out.println("No application.properties file found. Getting everything from the environment");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        try {
        	PrintWriter healthFileWriter = new PrintWriter(getProperty("output.directory") + "/health.txt");
        	healthFileWriter.append("I am healthy");
        	healthFileWriter.close();
        } catch(Exception e) {
        	e.printStackTrace();
        }
    }
	static public String getProperty(String propName) {
    	String value = System.getProperty(propName);
    	if(value == null) {
        	value = System.getenv(propName.replace(".", "_"));
        	if(value == null) {
        		value = appProps.getProperty(propName);
        	}
    	}
    	return value;
	}
	static public int getIntProperty(String propName) {
    	String value = getProperty(propName);
    	if(value != null) {
    		try {
    			return Integer.parseInt(value);
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	return 0;
	}
	static public boolean getBooleanProperty(String propName) {
    	String value = getProperty(propName);
    	if(value != null) {
    		try {
    			return Boolean.parseBoolean(value);
    		} catch(Exception e) {
    			e.printStackTrace();
    		}
    	}
    	return false;
	}
	static public String convertMillisecondsToTimeString(long milliseconds) {
		StringBuffer sb = new StringBuffer();
		long seconds = milliseconds / 1000;
		final long oneHour = 60 * 60;
		final long oneDay = 24 * oneHour;
		long days = seconds / oneDay;
		long hours = 0;
		long minutes = 0;
		seconds = (days > 0 ? seconds - (days * oneDay) : seconds);
		if(seconds > 0) {
			hours = seconds / oneHour;
			seconds = (hours > 0 ? seconds - (hours * oneHour) : seconds);
			if(seconds > 0) {
				minutes = seconds / 60;
				seconds = (minutes > 0 ? seconds - (minutes * 60) : seconds);
			}
		}
		if(days > 0) {
			sb.append(days + (days == 1 ? " day" : " days"));
		}
		if(hours > 0) {
			if(sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(hours + (hours == 1 ? " hour" : " hours"));
		}
		if(minutes > 0) {
			if(sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(minutes + (minutes == 1 ? " minute" : " minutes"));
		}
		if(seconds > 0) {
			if(sb.length() > 0) {
				sb.append(", ");
			}
			sb.append(seconds + (seconds == 1 ? " second" : " seconds"));
		}
		if(sb.length() == 0) {
			sb.append(milliseconds + " ms");
		}
		return sb.toString();
	}
}

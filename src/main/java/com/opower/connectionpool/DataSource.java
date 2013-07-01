package com.opower.connectionpool;

import java.io.IOException;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * A singleton DataSource instance bean that represents the properties provided in the datsource.properties file
 * 
 * @author VinayG
 */
class DataSource {
	
	private static final String DATASOURCE_DRIVER = "datasource.driver";
	private static final String DATASOURCE_URL = "datasource.url";
	private static final String DATASOURCE_USERNAME = "datasource.username";
	private static final String DATASOURCE_PASSWORD = "datasource.password";
	private static final String DATASOURCE_POOLSIZE = "datasource.poolsize";
	private static final String DATASOURCE_MAX_IDLE_TIME_IN_SECONDS = "datasource.maxIdleTimeInSeconds";
	
	protected String _driver;
	protected String _url;
	protected String _username;
	protected String _password;
	protected int _poolSize;
	protected int _maxIdleTimeInSeconds;
	
	private Logger log = Logger.getLogger(DataSource.class.getName());
	
	private static final DataSource dataSourceInstance = new DataSource();
	
	private DataSource(){
		initializeDataSource();
	}
	
	/* Loads the properties from the datasource.properties file which is expected to be in the classpath. */
	private void initializeDataSource() {
		Properties prop = new Properties();
    	try {
    		prop.load(DataSource.class.getClassLoader().getResourceAsStream("datasource.properties"));
 
    		_driver = prop.getProperty(DATASOURCE_DRIVER);
    		_url = prop.getProperty(DATASOURCE_URL);
    		_username = prop.getProperty(DATASOURCE_USERNAME);
    		_password = prop.getProperty(DATASOURCE_PASSWORD);
    		_poolSize = Integer.parseInt(prop.getProperty(DATASOURCE_POOLSIZE, "5").trim());
    		_maxIdleTimeInSeconds = Integer.parseInt(prop.getProperty(DATASOURCE_MAX_IDLE_TIME_IN_SECONDS, "5").trim());
    	} catch (IOException ex) {
    		log.error("datasource.properties could not be loaded. " + ex.getMessage());
        }
	}
	
	public static DataSource getInstance(){
		return DataSource.dataSourceInstance;
	}

}

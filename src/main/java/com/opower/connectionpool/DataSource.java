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
	
	protected String driver;
	protected String url;
	protected String username;
	protected String password;
	protected int poolSize;
	protected int maxIdleTimeInSeconds;
	
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
 
    		driver = prop.getProperty(DATASOURCE_DRIVER);
    		url = prop.getProperty(DATASOURCE_URL);
    		username = prop.getProperty(DATASOURCE_USERNAME);
    		password = prop.getProperty(DATASOURCE_PASSWORD);
    		poolSize = Integer.parseInt(prop.getProperty(DATASOURCE_POOLSIZE, "5").trim());
    		maxIdleTimeInSeconds = Integer.parseInt(prop.getProperty(DATASOURCE_MAX_IDLE_TIME_IN_SECONDS, "5").trim());
    	} catch (IOException ex) {
    		log.error("datasource.properties could not be loaded. " + ex.getMessage());
        }
	}
	
	public static DataSource getInstance(){
		return DataSource.dataSourceInstance;
	}

}

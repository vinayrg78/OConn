package com.opower.connectionpool;

import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.junit.After;
import org.junit.Before;

/*
 * The abstract super class with base behavior for child test classes.
 * Ensures that the pool is drained/destroyed for every test case.
 * 
 * @author VinayG
 */
public abstract class AbstractTestOConnectionPoolImpl extends TestCase{

	public OConnectionPoolImpl connectionPoolImpl;
	public DataSource dataSource;
	
	public static int LITTLE_MORE_THAN_MAX_IDLE_TIME;
	public static int LITTLE_LESS_THAN_MAX_IDLE_TIME;
	
	@Before
	public void setUp() throws Exception {
		dataSource = DataSource.getInstance();
		connectionPoolImpl = new OConnectionPoolImpl();
		
		LITTLE_MORE_THAN_MAX_IDLE_TIME = dataSource.maxIdleTimeInSeconds * 1000 + 1000;
		LITTLE_LESS_THAN_MAX_IDLE_TIME = dataSource.maxIdleTimeInSeconds * 1000 - 1000;
	}

	@After
	public void tearDown() throws Exception {
		recyclePool();
	}
	
	protected Connection[] getAllConnections() throws SQLException {
		Connection[] connArr = new Connection[dataSource.poolSize];
		for(int i = 0; i < dataSource.poolSize; i++){
			connArr[i] = connectionPoolImpl.getConnection();
			assertNotNull(connArr[i]);
		}
		return connArr;
	}

	
	/* recycles all the connections in the pool. */
	protected void recyclePool(){
		connectionPoolImpl.destroyPool();
		//connectionPoolImpl.initializePool();
	}
}

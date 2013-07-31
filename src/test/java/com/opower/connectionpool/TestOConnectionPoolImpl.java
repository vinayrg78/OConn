package com.opower.connectionpool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Random;

import org.junit.Test;

/*
 * The class that houses numerous use case scenarios for testing the connectionPool implementation.
 * 
 * @author VinayG
 */
public class TestOConnectionPoolImpl extends AbstractTestOConnectionPoolImpl{

	/* Ensures proper working of the getConnection flow. */
	@Test
	public void testGetConnection() {
		try {
			Connection conn = connectionPoolImpl.getConnection();
			assertNotNull(conn);
			assertEquals(false, conn.isClosed());
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures that the pool doesn't give out more connections that its capacity.  */
	@Test
	public void testGetConnectionMax() {
		try {
			Connection[] connArr = getAllConnections();
			assertEquals(connArr.length, dataSource.poolSize);
			connectionPoolImpl.getConnection();
			fail("Should have thrown a IllegalStateException");
		} catch (SQLException e) {
			fail(e.getMessage());
		} catch (IllegalStateException e){
			//part of legit flow.
		}
	}

	/* Ensures releaseConnection returns the connection to the pool. */
	@Test
	public void testReleaseConnectionRestoresToPool() {
		Connection conn = null;
		try {
			Connection[] connArr = getAllConnections();
			for(int i = 0; i < connArr.length; i++){
				connectionPoolImpl.releaseConnection(connArr[i]);
				assertEquals(connArr[i].isClosed(), true);
			}
			
			conn = connectionPoolImpl.getConnection();
			assertNotNull(conn);
			assertEquals(false, conn.isClosed());
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}
	
	/*
	 * Ensures that calling close on a connection restores the conn to the pool.
	 * Additionally the conn at hand should have isClosed() return true. 
	 */
	@Test
	public void testCloseConnection(){
		try {
			Connection[] connArr = getAllConnections();
			for(int i = 0; i < dataSource.poolSize; i++){
				connArr[i].close();
				assertEquals(connArr[i].isClosed(), true);
			}
			
			Connection newConn = connectionPoolImpl.getConnection();
			assertNotNull(newConn);
			assertEquals(newConn.isClosed(), false);
		} catch (SQLException e) {
			fail(e.getMessage());
		} catch (IllegalStateException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures that releaseConection closes the connection.   */
	@Test
	public void testReleaseConnectionClosesConnection() {
		Connection conn = null;
		try {
			conn = connectionPoolImpl.getConnection();
			assertEquals(conn.isClosed(), false);
			
			connectionPoolImpl.releaseConnection(conn);
			assertEquals(conn.isClosed(), true);
			
			conn.commit();
			fail("Should have thrown a IllegalStateException");
		} catch (SQLException e) {
			fail(e.getMessage());
		} catch (IllegalStateException e){
			//part of legit flow.
		}
	}
	
	/* Ensures releaseConnection can be invoked multiple times without any side effects. */
	@Test
	public void testReleaseConnectionsIdempotence() {
		Connection conn = null;
		try {
			conn = connectionPoolImpl.getConnection();
			assertEquals(conn.isClosed(), false);
			
			connectionPoolImpl.releaseConnection(conn);
			assertEquals(conn.isClosed(), true);

			connectionPoolImpl.releaseConnection(conn);
			assertEquals(conn.isClosed(), true);
			
			connectionPoolImpl.releaseConnection(conn);
			assertEquals(conn.isClosed(), true);
			
			//Ensure poolsize.
			Connection[] conArr = getAllConnections();
			assertEquals(conArr.length, dataSource.poolSize);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures releaseConnection and connection.close can be invoked (interchangeably) multiple times without any side effects. */
	@Test
	public void testReleaseAndCloseConnectionsIdempotence() {
		Connection conn = null;
		try {
			conn = connectionPoolImpl.getConnection();
			assertEquals(conn.isClosed(), false);
			
			conn.close();
			assertEquals(conn.isClosed(), true);

			connectionPoolImpl.releaseConnection(conn);
			assertEquals(conn.isClosed(), true);
			connectionPoolImpl.releaseConnection(conn);
			assertEquals(conn.isClosed(), true);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures close can be invoked multiple times without any side effects. */
	@Test
	public void testCloseConnectionsIdempotence() {
		Connection conn = null;
		try {
			conn = connectionPoolImpl.getConnection();
			assertEquals(conn.isClosed(), false);
			
			conn.close();
			assertEquals(conn.isClosed(), true);
			conn.close();
			assertEquals(conn.isClosed(), true);
			conn.close();
			assertEquals(conn.isClosed(), true);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}

	/* Ensures abandoned connections are successfully recovered by the pool. */
	@Test
	public void testRelaseOfAbandonedConnection(){
		try {
			Connection[] connArr = getAllConnections();
			if(connArr[0] != null){
				connectionPoolImpl.releaseConnection(connArr[0]);
				assertEquals(connArr[0].isClosed(), true);
			}
			
			for(int i = 1; i < connArr.length; i++){
				assertEquals(connArr[i].isClosed(), false);
			}
			Thread.sleep(LITTLE_MORE_THAN_MAX_IDLE_TIME);

			for(int i = 1; i < connArr.length; i++){
				assertEquals(connArr[i].isClosed(), true);
			}
		} catch (SQLException e) {
			fail(e.getMessage());
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures abandoned connections that used to be active are successfully recovered by the pool. */
	@Test
	public void testRelaseOfAbandonedConnectionsAfterActivity(){
		try {
			Connection[] connArr = getAllConnections();
			for(int i = 0; i < 4; i++){
				Thread.sleep(LITTLE_LESS_THAN_MAX_IDLE_TIME);
				for(int j = 0; j < connArr.length; j++){
					assertEquals(connArr[j].isClosed(), false);
				}
			}
			
			Thread.sleep(LITTLE_MORE_THAN_MAX_IDLE_TIME);
			for(int j = 0; j < connArr.length; j++){
				assertEquals(connArr[j].isClosed(), true);
			}
		} catch (SQLException e) {
			fail(e.getMessage());
		} catch (InterruptedException e) {
			fail(e.getMessage());
		}
	}
	
	/* A simulation of multiple threads accessing the connection pool.
	 * The test ascertains that at the pool size is always between 0 and the maxPoolSize.
	 * 
	 * While it is arguable that using unit tests for concurrency issues is not consistent and that better frameworks for doing so exist;
	 * this test was added to validate a very basic predicate of the pool and serves that purpose to a reasonable extent.
	 */
	public void testMultiThreadEnvironment(){
		try {
			for(int i = 0 ; i < 5; i++){
				Thread t = new Thread(new PoolUser());
				t.setName("T" + i);
				t.start();
			}
			Thread.sleep(4000);
			for(int i = 5 ; i < 10; i++){
				Thread t = new Thread(new PoolUser());
				t.setName("T" + i);
				t.start();
			}
			Thread.sleep(4000);
			for(int i = 10 ; i < 15; i++){
				Thread t = new Thread(new PoolUser());
				t.setName("T" + i);
				t.start();
			}
			Thread.sleep(4000);
			for(int i = 16 ; i < 20; i++){
				Thread t = new Thread(new PoolUser());
				t.setName("T" + i);
				t.start();
			}
			Thread.sleep(18000);
			assertTrue(((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() <= dataSource.poolSize &&
					((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() >= 0);
		} catch (InterruptedException e1) {
			fail("Unexpected Error: " + e1.getMessage());
		}
	}
	
	class PoolUser implements Runnable {
		public void run(){
			try {
				assertTrue(((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() <= dataSource.poolSize &&
						((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() >= 0);
				Connection conn = connectionPoolImpl.getConnection();
				conn.isClosed();
				//sleep between 1 to 5 seconds.
				int randomNum = new Random().nextInt(5) + 1;
				Thread.sleep(randomNum * 1000);
				assertTrue(((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() <= dataSource.poolSize &&
						((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() >= 0);
				conn.getAutoCommit();
				connectionPoolImpl.releaseConnection(conn);
				//conn.close();
				assertTrue(((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() <= dataSource.poolSize &&
						((OConnectionPoolImpl)connectionPoolImpl).getNumberOfAvailableConnections() >= 0);
			} catch (SQLException e) {
				fail("Unexpected error: "  + Thread.currentThread().getName() + e.getMessage());
			} catch (IllegalStateException ex){
				//This exception can be part of the legit flows.
			} catch (InterruptedException e) {
				fail("Unexpected error: "  + Thread.currentThread().getName() + e.getMessage());
			}
		}
	}
	
}




package com.opower.connectionpool;

import java.sql.Connection;
import java.sql.SQLException;

import junit.framework.TestCase;

import org.easymock.classextension.EasyMock;
import org.junit.Before;
import org.junit.Test;

/**
 * MockTests to ensure the proper hooks are in place in the wrapperConnection (OConnection).
 * The close and isClosed are the primary hook points.
 * 
 * @author VinayG
 */
public class TestOConnection extends TestCase {

	private Connection connectionMock;
	private OConnectionPoolImpl connectionPoolMock;
	private Connection oConnection;
	
	
	@Before
	public void setUp() throws Exception {
		connectionMock = EasyMock.createStrictMock(Connection.class);
		connectionPoolMock = EasyMock.createStrictMock(OConnectionPoolImpl.class);
		oConnection = new OConnection(connectionMock, connectionPoolMock);
	}
	
	
	/* Ensures that the pool's resetAbandonedCheckTimer is invoked when close is called on the OConnection instance. */
	@Test
	public void testClose() {
		try {
			((OConnectionPoolImpl)connectionPoolMock).resetAbandonedCheckTimer((OConnection) oConnection);
			EasyMock.expectLastCall();
			
			connectionMock.close();
			EasyMock.expectLastCall();

			connectionPoolMock.releaseConnection((OConnection) oConnection);
			EasyMock.expectLastCall();
			
			EasyMock.replay(connectionMock);
			EasyMock.replay(connectionPoolMock);
			
	        oConnection.close();
	        EasyMock.verify(connectionMock);
	        EasyMock.verify(connectionPoolMock);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures that the pool's resetAbandonedCheckTimer is invoked and the physical connection's
	 * isClosed() is called when isClosed is invoked on the OConnection instance. */
	@Test
	public void testIsClosed() {
		try {
			((OConnectionPoolImpl)connectionPoolMock).resetAbandonedCheckTimer((OConnection) oConnection);
			EasyMock.expectLastCall();
			
			EasyMock.expect(connectionMock.isClosed()).andReturn(false);
			EasyMock.expectLastCall();

			EasyMock.replay(connectionMock);
			EasyMock.replay(connectionPoolMock);
			
	        oConnection.isClosed();
	        EasyMock.verify(connectionMock);
	        EasyMock.verify(connectionPoolMock);
		} catch (SQLException e) {
			fail(e.getMessage());
		}	
	}
	
}

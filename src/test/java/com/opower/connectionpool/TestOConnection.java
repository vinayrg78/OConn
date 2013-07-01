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

	private Connection _connectionMock;
	private OConnectionPoolImpl _connectionPoolMock;
	private Connection oConnection;
	
	
	@Before
	public void setUp() throws Exception {
		_connectionMock = EasyMock.createStrictMock(Connection.class);
		_connectionPoolMock = EasyMock.createStrictMock(OConnectionPoolImpl.class);
		oConnection = new OConnection(_connectionMock, _connectionPoolMock);
	}
	
	
	/* Ensures that the pool's resetAbandonedCheckTimer is invoked when close is called on the OConnection instance. */
	@Test
	public void testClose() {
		try {
			((OConnectionPoolImpl)_connectionPoolMock).resetAbandonedCheckTimer((OConnection) oConnection);
			EasyMock.expectLastCall();
			
			_connectionMock.close();
			EasyMock.expectLastCall();

			_connectionPoolMock.releaseConnection((OConnection) oConnection);
			EasyMock.expectLastCall();
			
			EasyMock.replay(_connectionMock);
			EasyMock.replay(_connectionPoolMock);
			
	        oConnection.close();
	        EasyMock.verify(_connectionMock);
	        EasyMock.verify(_connectionPoolMock);
		} catch (SQLException e) {
			fail(e.getMessage());
		}
	}
	
	/* Ensures that the pool's resetAbandonedCheckTimer is invoked and the physical connection's
	 * isClosed() is called when isClosed is invoked on the OConnection instance. */
	@Test
	public void testIsClosed() {
		try {
			((OConnectionPoolImpl)_connectionPoolMock).resetAbandonedCheckTimer((OConnection) oConnection);
			EasyMock.expectLastCall();
			
			EasyMock.expect(_connectionMock.isClosed()).andReturn(false);
			EasyMock.expectLastCall();

			EasyMock.replay(_connectionMock);
			EasyMock.replay(_connectionPoolMock);
			
	        oConnection.isClosed();
	        EasyMock.verify(_connectionMock);
	        EasyMock.verify(_connectionPoolMock);
		} catch (SQLException e) {
			fail(e.getMessage());
		}	
	}
	
}

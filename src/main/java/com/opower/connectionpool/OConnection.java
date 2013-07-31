package com.opower.connectionpool;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.Map;
import java.util.Properties;

/**
 * Wrapper class for Connection Object. Primarily "forwards" the method invocations to the
 * wrapped physical connection. 
 * 
 * The purpose of using the wrapper is to ensure the pool has control over the lifecycle of the connections.
 * This design also facilitates invoking the pool features as and when required.
 * 
 * @author VinayG
 */
public class OConnection implements Connection {

	//The physical connection wrapped by this instance
	protected Connection _connection;
	
	//The pool to which this connection belongs.
	private final ConnectionPool _connectionPool;
	
	public OConnection(Connection connection, ConnectionPool connectionPool){
		this._connection = connection;
		this._connectionPool = connectionPool;
	}
	
	/**
	 * In addition to asserting that the underlying connection is not null this also invokes the 
	 * resetAbandonedCheckTimer() in order to restart the Abandoned Check Timer. This means that as long as methods on the
	 * borrowed connection are being invoked, the client can keep the connection object for as long as desired.
	 * On the other hand, not invoking any methods would forcibly close the connection and reinsert it into the pool.
	 * 
	 * @throws IllegalStateException
	 */
	private void checkConnection() throws IllegalStateException {
		if(_connection == null){
			throw new IllegalStateException("This connection may have already been closed or released.");
		}
		if(_connectionPool instanceof OConnectionPoolImpl){
			((OConnectionPoolImpl)_connectionPool).resetAbandonedCheckTimer(this);
		}
	}
	
	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		checkConnection();
		return _connection.isWrapperFor(arg0);
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		checkConnection();
		return _connection.unwrap(arg0);
	}

	@Override
	public void clearWarnings() throws SQLException {
		checkConnection();
		_connection.clearWarnings();
	}

	/*
	 * Invoking releaseConnection on close so that the connection can be promptly reinserted into the pool.
	 * This would ensure that closed connections that arent returned dont result in a smaller pool until they are detected as abandoned.
	 */
	@Override
	public void close() throws SQLException {
		if(_connection == null){
			return;
		}
		checkConnection();
		_connection.close();
		_connectionPool.releaseConnection(this);
	}

	@Override
	public void commit() throws SQLException {
		checkConnection();
		_connection.commit();
	}

	@Override
	public Array createArrayOf(String typeName, Object[] elements)
			throws SQLException {
		checkConnection();
		return _connection.createArrayOf(typeName, elements);
	}

	@Override
	public Blob createBlob() throws SQLException {
		checkConnection();
		return _connection.createBlob();
	}

	@Override
	public Clob createClob() throws SQLException {
		checkConnection();
		return _connection.createClob();
	}

	@Override
	public NClob createNClob() throws SQLException {
		checkConnection();
		return _connection.createNClob();
	}

	@Override
	public SQLXML createSQLXML() throws SQLException {
		checkConnection();
		return _connection.createSQLXML();
	}

	@Override
	public Statement createStatement() throws SQLException {
		checkConnection();
		return _connection.createStatement();
	}

	@Override
	public Statement createStatement(int resultSetType, int resultSetConcurrency)
			throws SQLException {
		checkConnection();
		return _connection.createStatement(resultSetType, resultSetConcurrency);
	}

	@Override
	public Statement createStatement(int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkConnection();
		return _connection.createStatement(resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public Struct createStruct(String typeName, Object[] attributes)
			throws SQLException {
		checkConnection();
		return _connection.createStruct(typeName, attributes);
	}

	@Override
	public boolean getAutoCommit() throws SQLException {
		checkConnection();
		return _connection.getAutoCommit();
	}

	@Override
	public String getCatalog() throws SQLException {
		checkConnection();
		return _connection.getCatalog();
	}

	@Override
	public Properties getClientInfo() throws SQLException {
		checkConnection();
		return _connection.getClientInfo();
	}

	@Override
	public String getClientInfo(String name) throws SQLException {
		checkConnection();
		return _connection.getClientInfo(name);
	}

	@Override
	public int getHoldability() throws SQLException {
		checkConnection();
		return _connection.getHoldability();
	}

	@Override
	public DatabaseMetaData getMetaData() throws SQLException {
		checkConnection();
		return _connection.getMetaData();
	}

	@Override
	public int getTransactionIsolation() throws SQLException {
		checkConnection();
		return _connection.getTransactionIsolation();
	}

	@Override
	public Map<String, Class<?>> getTypeMap() throws SQLException {
		checkConnection();
		return _connection.getTypeMap();
	}

	@Override
	public SQLWarning getWarnings() throws SQLException {
		checkConnection();
		return _connection.getWarnings();
	}

	/* Returns true if the wrapped physical connection has been set to null (i.e: connection has been returned to pool).
	 * else forwards on to wrapped connection. */
	@Override
	public boolean isClosed() throws SQLException {
		if(_connection == null){
			return true;
		}
		checkConnection();
		return _connection.isClosed();
	}

	@Override
	public boolean isReadOnly() throws SQLException {
		checkConnection();
		return _connection.isReadOnly();
	}

	@Override
	public boolean isValid(int timeout) throws SQLException {
		checkConnection();
		return _connection.isValid(timeout);
	}

	@Override
	public String nativeSQL(String sql) throws SQLException {
		checkConnection();
		return _connection.nativeSQL(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql) throws SQLException {
		checkConnection();
		return _connection.prepareCall(sql);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		checkConnection();
		return _connection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public CallableStatement prepareCall(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkConnection();
		return _connection.prepareCall(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public PreparedStatement prepareStatement(String sql) throws SQLException {
		checkConnection();
		return _connection.prepareStatement(sql);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
			throws SQLException {
		checkConnection();
		return _connection.prepareStatement(sql, autoGeneratedKeys);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
			throws SQLException {
		checkConnection();
		return _connection.prepareStatement(sql, columnIndexes);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, String[] columnNames)
			throws SQLException {
		checkConnection();
		return _connection.prepareStatement(sql, columnNames);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency) throws SQLException {
		checkConnection();
		return _connection.prepareStatement(sql, resultSetType, resultSetConcurrency);
	}

	@Override
	public PreparedStatement prepareStatement(String sql, int resultSetType,
			int resultSetConcurrency, int resultSetHoldability)
			throws SQLException {
		checkConnection();
		return _connection.prepareStatement(sql, resultSetType, resultSetConcurrency, resultSetHoldability);
	}

	@Override
	public void releaseSavepoint(Savepoint savepoint) throws SQLException {
		checkConnection();
		_connection.releaseSavepoint(savepoint);
	}

	@Override
	public void rollback() throws SQLException {
		checkConnection();
		_connection.rollback();
	}

	@Override
	public void rollback(Savepoint savepoint) throws SQLException {
		checkConnection();
		_connection.releaseSavepoint(savepoint);
	}

	@Override
	public void setAutoCommit(boolean autoCommit) throws SQLException {
		checkConnection();
		_connection.setAutoCommit(autoCommit);
	}

	@Override
	public void setCatalog(String catalog) throws SQLException {
		checkConnection();
		_connection.setCatalog(catalog);
	}

	@Override
	public void setClientInfo(Properties properties)
			throws SQLClientInfoException {
		checkConnection();
		_connection.setClientInfo(properties);
	}

	@Override
	public void setClientInfo(String name, String value)
			throws SQLClientInfoException {
		checkConnection();
		_connection.setClientInfo(name, value);
	}

	@Override
	public void setHoldability(int holdability) throws SQLException {
		checkConnection();
		_connection.setHoldability(holdability);
	}

	@Override
	public void setReadOnly(boolean readOnly) throws SQLException {
		checkConnection();
		_connection.setReadOnly(readOnly);
	}

	@Override
	public Savepoint setSavepoint() throws SQLException {
		checkConnection();
		return _connection.setSavepoint();
	}

	@Override
	public Savepoint setSavepoint(String name) throws SQLException {
		checkConnection();
		return _connection.setSavepoint(name);
	}

	@Override
	public void setTransactionIsolation(int level) throws SQLException {
		checkConnection();
		_connection.setTransactionIsolation(level);
	}

	@Override
	public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
		checkConnection();
		_connection.setTypeMap(map);
	}

}

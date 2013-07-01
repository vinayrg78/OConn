package com.opower.connectionpool;

import org.junit.Test;

public class TestDataSource extends AbstractTestOConnectionPoolImpl{

	
	/* Ensures that multiple invocations retrieve the same singleton instance of Datasource. */
	@Test
	public void testGetInstance() {
		assertEquals(DataSource.getInstance(), dataSource);
	}

}

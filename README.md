
PROJECT TITLE: OConn
Author: Vinay Gangoli
Date: 06-30-2013

Overview: 
This connection pool submission consists of a OConnectionPoolImpl class which implements the given ConnectionPool interface.
The pool uses a LinkedList data structure to maintain the pool. It schedules a time delayed future task to reclaim the connection 
if not utilized for the max idle time period. Every time the connection is utilized the timer is reset. This ensures that clients 
who use their connections can continue to hold on to them.

This software does not intend to support multiple connection pools hence this class is a singleton.
The DataSource bean is also a singleton used to represent the properties set in the datasource.properties by the client/user.
OConnection is a wrapper class which wraps the java.sql.Connection object in it. This helps the pool control the lifecycle and
certain diagnostic information about the connections that are given out. One proactive feature is the ability of the connection to return
itself to the pool is close is invoked on it. This is done to prevent closed connections from impacting pool capacity.

Junit and EasyMock Test cases have been included. Please not that running the test (i.e: mvn test) takes  upto 20 seconds.
This is primarily due to the nature of certain tests that cause the thread to sleep to test events like abandonment.
Logs are written to the ConnectionPool.log file which will be created under the OConn folder.



How to configure:
The datasource.properties file under src/main/resources contains all the properties needed to configure the ConnectionPool.
The poolsize is the maximum number of connections the pool is allowed to create.
The maxIdleTimeInSeconds is the maximum amount of time for which a borrowed connection can remain idle before it is considered abandoned.
Both these properties are optional and will default to 5.
Rest of the properties are mandatory and self-explanatory.



How to execute the test cases: 
After exploding the zip, all the files should be under a OConn folder.
Simply run the following from under OConn:
$> mvn compile
$> mvn test



Other notes: The Eclipse IDE (Java 6) was used to develop this software. In addition to the pre-existing dependencies easymockclassextension 
(with it's dependencies) was added to facilitate the writing of certain mock tests.

Thank you for reviewing this software.





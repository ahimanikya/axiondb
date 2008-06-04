/*
 * $Id: TestAxionWithIndex.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above
 *    copyright notice, this list of conditions and the following
 *    disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The names "Tigris", "Axion", nor the names of its contributors may
 *    not be used to endorse or promote products derived from this
 *    software without specific prior written permission.
 *
 * 4. Products derived from this software may not be called "Axion", nor
 *    may "Tigris" or "Axion" appear in their names without specific prior
 *    written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A
 * PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 * =======================================================================
 */

package org.axiondb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.axiondb.jdbc.AxionDriver;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @author william.lam
 * @version $Revision: 1.1 $
 */
public class TestAxionWithIndex extends TestCase {

private Connection _connection;
private String _tableName = "testTable";
private Object[] _keys = new Object[4]; 
private String [] _values = new String[4]; 


public static void main(String[] args) {
TestRunner.run(suite());
}

public static Test suite() {
return new TestSuite(TestAxionWithIndex.class);
}

public TestAxionWithIndex(String testName) {
super(testName);
}

public void setUp() throws Exception {
super.setUp();
        
        // lets class load the driver
        //Thread.currentThread().getContextClassLoader().loadClass(AxionDriver.class.getName());
        getClass().getClassLoader().loadClass(AxionDriver.class.getName());
        
        _connection = DriverManager.getConnection("jdbc:axiondb:" + _tableName +":axiondir");
        //connection = DriverManager.getConnection("jdbc:axiondb:memdb");

// create table
createTable(_tableName);

for (int i = 0; i < _keys.length; i++ ) {
_keys[i] = createKey(i);
_values[i] = String.valueOf(i); 
}
}

public void tearDown() throws Exception {
dropTable(_tableName);

_connection.close();
}

/**
 * @param tableName
 */
private void dropTable(String tableName) {
String sql;
Statement statement = null;
try {
statement = _connection.createStatement();

sql = "drop table " + tableName;
statement.execute(sql);
}
catch (SQLException e) {
}
finally {
if (statement != null) {

try {
statement.close();
}
catch (Exception e) {
// ignore errors
}
}
}
}

/**
 * @param tableName
 */
private void createTable(String tableName) {
Statement statement = null;
String sql;
try {
sql =
"create table "
+ tableName
+ "( key_object java_object, entry java_object )";

statement = _connection.createStatement();
statement.execute(sql);

sql =
"create index "
+ tableName
+ "_pk on "
+ tableName
+ " ( key_object )";
/*
 * Create index for this table
 * 
 */
// THE TEST WILL PASS IF TABLE INDEX IS NOT CREATED
statement.execute(sql);


}
catch (SQLException sqle) {
System.err.println("Cannot create TABLE!" + sqle);
}
finally {
if (statement != null) {

try {
statement.close();
}
catch (Exception e) {
// ignore errors
}
}
}
}

public Object get(Object key) {
PreparedStatement statement = null;
ResultSet resultSet = null;
Object entry = null;
String sql =
"Select entry From " + _tableName + " Where key_object = ?";

try {
statement = _connection.prepareStatement(sql);
statement.setObject(1, key);
resultSet = statement.executeQuery();
if (resultSet.next()) {
entry = resultSet.getObject(1);
}
}
catch (Exception e) {
e.printStackTrace();
}
return entry;
}

public boolean put(Object key, Object entry) {
PreparedStatement statement = null;
String sql =
"Insert Into " + _tableName + " (entry, key_object) Values (?, ?)";

try {
statement = _connection.prepareStatement(sql);
statement.setObject(1, entry);
statement.setObject(2, key);
assertEquals(1,statement.executeUpdate());
return true;
}
catch (Exception e) {
e.printStackTrace();
}
return false;
}


public void testRun() {
assertNotNull("Connection is null", _connection);

assertTrue("Failed putting key 0", put(_keys[0], _values[0]));
assertTrue("Failed putting key 1", put(_keys[1], _values[1]));
assertTrue("Failed putting key 2", put(_keys[2], _values[2]));

assertEquals("Invalid Key 0 value", _values[0], get(_keys[0]));
assertEquals("Invalid Key 1 value", _values[1], get(_keys[1]));
assertEquals("Invalid Key 2 value", _values[2], get(_keys[2]));
}

/**
 * Factory method to create a key instance
 * 
 * @param i
 * @return
 */
protected Object createKey(int i) {
//  THIS TEST WILL PASS IF THE KEY IS A String!
// return "key" + i;

return new Integer(i);
}
}

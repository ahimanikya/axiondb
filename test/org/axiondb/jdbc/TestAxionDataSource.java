/*
 * $Id: TestAxionDataSource.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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

package org.axiondb.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Rod Waldhoff
 */
public class TestAxionDataSource extends TestCase {
   private DataSource _dataSource = null;

   public TestAxionDataSource(String testName) {
      super(testName);
   }

   public static void main(String args[]) {
      TestRunner.run( suite() );
   }

   public static Test suite() {
      return new TestSuite(TestAxionDataSource.class);
   }

   public void setUp() throws SQLException {
      _dataSource = new AxionDataSource("jdbc:axiondb:memdb");
   }

   public void tearDown() {
   }

   public void testConnect() throws SQLException {
      assertNotNull(_dataSource.getConnection());
   }

   public void testConnectWithUnamePasswd() throws SQLException {
      assertNotNull(_dataSource.getConnection("uname","passwd"));
   }

   public void testGetAndSetLoginTimeout() throws SQLException {
      assertEquals(0,_dataSource.getLoginTimeout());
      _dataSource.setLoginTimeout(100);
      assertEquals(100,_dataSource.getLoginTimeout());
   }

   public void testGetAndSetLogWriter() throws SQLException {
      assertNull(_dataSource.getLogWriter());
      PrintWriter writer = new PrintWriter(new ByteArrayOutputStream());
      _dataSource.setLogWriter(writer);
      assertSame(writer,_dataSource.getLogWriter());
      _dataSource.setLogWriter(null);
      assertNull(_dataSource.getLogWriter());
   }

   public void testGetConnectionWithInvalidConnectString() throws SQLException {
       AxionDataSource ds = new AxionDataSource(";");       
       try {
           ds.getConnection();
           fail("Expected SQLException");
       } catch(SQLException e) {
           // expected
       }
   }
}

/*
 * $Id: TestAxionDriver.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:37 $
 * @author Chuck Burdick
 */
public class TestAxionDriver extends TestCase {
   private Driver _driver = null;

   public TestAxionDriver(String testName) {
      super(testName);
   }

   public static void main(String args[]) {
      TestRunner.run( suite() );
   }

   public static Test suite() {
      return new TestSuite(TestAxionDriver.class);
   }

   public void setUp() throws SQLException {
      _driver = new AxionDriver();
   }

   public void tearDown() {
   }

   public void testAcceptSimple() throws SQLException {
      assertTrue("Should have accepted", _driver.acceptsURL("jdbc:axiondb:memdb"));
   }

   public void testConnect() throws SQLException {
      assertNotNull(_driver.connect("jdbc:axiondb:memdb",null));
   }

   public void testDenyBogus() throws SQLException {
      assertTrue("Shouldn't have accepted", !_driver.acceptsURL("jdbc:bogus:."));
   }

   public void testConnectReturnsNullForBadURL() throws SQLException {
      assertNull("Should get null", _driver.connect("jdbc:bogus:.",null));
   }

   public void testAttributes() {
      assertEquals("Should get major version", 0, _driver.getMajorVersion());
      assertEquals("Should get minor version", 9, _driver.getMinorVersion());
      assertTrue("Is not yet JDBC compliant", !_driver.jdbcCompliant());
   }

   public void testPropertyInfo() throws SQLException {
      DriverPropertyInfo[] info = _driver.getPropertyInfo(null, null);
      assertNotNull("Should get info back", info);
      assertEquals("Should have no entries", 0, info.length);
   }

}

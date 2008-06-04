/*
 * $Id: TestAxionResultSet_Disk.java,v 1.1 2007/11/28 10:01:37 jawed Exp $
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

import java.io.File;

import org.axiondb.io.FileUtil;

import junit.framework.Test;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * Tests AxionResultSet against a (@link org.axiondb.engine.tables.DiskTable}.
 * 
 * @author Jonathan Giron
 * @version $Revision: 1.1 $
 */
public class TestAxionResultSet_Disk extends TestAxionResultSet {

    private static final String DATABASE_NAME = "JDBC_DiskDB";
    
    public TestAxionResultSet_Disk(String testName) {
        super(testName);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }

    public static Test suite() {
        return new TestSuite(TestAxionResultSet_Disk.class);
    }    

    protected void doCleanup() throws Exception {
        if (_rset != null) {
            _rset.close();
        }
        
        if (_stmt != null) {
            _stmt.execute("drop table foo");
            _stmt.close();
        }
        
        if (_conn != null) {
            _conn.close();
        }
        
        FileUtil.delete(getDatabaseDirectory());
    }

    protected String getDatabaseName() {
        return DATABASE_NAME;
    }
    
    protected File getDatabaseDirectory() {
        return new File(".", DATABASE_NAME);
    }
}

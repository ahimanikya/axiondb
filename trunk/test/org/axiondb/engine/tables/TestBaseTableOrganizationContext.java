/*
 * $Id: TestBaseTableOrganizationContext.java,v 1.1 2007/11/28 10:01:27 jawed Exp $
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.tables;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:27 $
 * @author Rodney Waldhoff
 */
public class TestBaseTableOrganizationContext extends TestCase {

    //------------------------------------------------------------ Conventional

    public TestBaseTableOrganizationContext(String testName) {
        super(testName);
    }

    public static Test suite() {
        TestSuite suite = new TestSuite(TestBaseTableOrganizationContext.class);
        return suite;
    }
    
    public void setUp() throws Exception {
        _table = new BaseTableOrganizationContext() {
            
            private Set _propertyKeys = new HashSet();
            {
                _propertyKeys.add("propertyKeyRequired1");
                _propertyKeys.add("propertyKeyRequired2");
                _propertyKeys.add("propertyKeyOptional");
            }
            private Set _requiredPropertyKeys = new HashSet();
            {
                _requiredPropertyKeys.add("propertyKeyRequired1");
                _requiredPropertyKeys.add("propertyKeyRequired2");
            }

            public Properties getTableProperties() {
                return null;
            }

            public Set getPropertyKeys() {                
                return _propertyKeys;
            }

            public Set getRequiredPropertyKeys() {
                return _requiredPropertyKeys;
            }
            
            public void updateProperties() {
                
            }
            
            public void readOrSetDefaultProperties(Properties props) throws AxionException {
                
            }

        };
    }

    private BaseTableOrganizationContext _table = null;
    
    // tests
    //-------------------------------------------------------------------------
    
    public void testInvalidPropertyKey() {
        Properties props = new Properties();
        props.put("invalidKey","value");
        props.put("propertyKeyRequired1","value");
        props.put("propertyKeyRequired2","value");
        try {
            _table.assertValidPropertyKeys(props);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }
    
    public void testMissingPropertyKey() {
        Properties props = new Properties();
        props.put("propertyKeyOptional","value");
        try {
            _table.assertValidPropertyKeys(props);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }
    
    public void testMissingPropertyKeys() {
        Properties props = new Properties();
        try {
            props.put("propertyKeyRequired1","value");
            _table.assertValidPropertyKeys(props);
            fail("Expected AxionException");
        } catch(AxionException e) {
            // expected
        }
    }

    public void testValidPropertyKeys() throws AxionException {
        Properties props = new Properties();
        props.put("propertyKeyRequired1","value");
        props.put("propertyKeyRequired2","value");
        _table.assertValidPropertyKeys(props);
        props.put("propertyKeyOptional","value");
        _table.assertValidPropertyKeys(props);
    }
}


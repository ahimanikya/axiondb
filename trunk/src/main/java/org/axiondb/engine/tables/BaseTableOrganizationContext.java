/*
 * 
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

import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.ExternalTable;
import org.axiondb.TableOrganizationContext;

/**
 * Table Organization Context.
 * 
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 * @version  
 */
public abstract class BaseTableOrganizationContext implements TableOrganizationContext {

    private Set PROPERTY_KEYS = new HashSet(2);

    public BaseTableOrganizationContext() {
        // Build set of recognized property keys for external tables.
        PROPERTY_KEYS.add(ExternalTable.PROP_LOADTYPE);
        PROPERTY_KEYS.add(ExternalTable.COLUMNS_ARE_CASE_SENSITIVE);
    }

    public Properties getTableProperties() {
        return _props;
    }

    public abstract Set getPropertyKeys();

    public abstract Set getRequiredPropertyKeys();
    
    public abstract void readOrSetDefaultProperties(Properties props) throws AxionException;

    public void updateProperties() {
        _props.clear();
    }

    public void setProperty(String key, String value) {
        if(value == null) {
            value ="";
        }
        _props.setProperty(key, value);
    }

    public Set getBasePropertyKeys() {
        return Collections.unmodifiableSet(PROPERTY_KEYS);
    }

    public Set getBaseRequiredPropertyKeys() {
        return Collections.EMPTY_SET;
    }

    public void assertValidPropertyKeys(Properties props) throws AxionException {
        Enumeration keys = props.keys();
        Set validKeys = getPropertyKeys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            if (!validKeys.contains(key)) {
                throw new AxionException("Property '" + key
                    + "' not recognized for this table type.");
            }
        }

        Set requiredKeys = new HashSet(getRequiredPropertyKeys());
        if (!(props.keySet().containsAll(requiredKeys))) {
            requiredKeys.removeAll(props.keySet());

            StringBuffer errMsgBuf = new StringBuffer(
                "Missing required properties in organization clause:  ");
            Iterator iter = requiredKeys.iterator();
            int k = 0;
            while (iter.hasNext()) {
                if (k++ != 0) {
                    errMsgBuf.append(", ");
                }
                errMsgBuf.append(iter.next());
            }

            throw new AxionException(errMsgBuf.toString());
        }
    }
    
    protected Properties _props = new Properties(); 
}

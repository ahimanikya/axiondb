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
package org.axiondb;

import java.util.Properties;
import java.util.Set;


/**
 * Table Organization Context.
 * 
 * @author Ahimanikya Satapathy
 * @version  
 */
public interface TableOrganizationContext {
    /**
     * Gets table properties for this external table instance.
     * 
     * @return Properties instance containing current table properties
     */
    Properties getTableProperties();

    /**
     * Gets a Set of Strings representing valid property key names.
     * 
     * @return Set of valid property key names
     */
    Set getPropertyKeys();

    Set getRequiredPropertyKeys();

    /**
     * Gets a Set of Strings representing property key names that all ExternalTable
     * instances should accept.
     * 
     * @return Set of basic valid property key names
     */
    Set getBasePropertyKeys();

    /**
     * Gets a Set of Strings representing property key names that all ExternalTable
     * instances must require.
     * 
     * @return Set of basic required property key names
     */
    Set getBaseRequiredPropertyKeys();

    /**
     * Asserts that all property keys referenced in the given Properties instance are
     * valid for the specific external table type.
     * 
     * @param props Properties instance whose keys are to be checked
     * @throws AxionException
     */
    void assertValidPropertyKeys(Properties props) throws AxionException;
    
    void readOrSetDefaultProperties(Properties props) throws AxionException;
    
    void updateProperties();
    
    void setProperty(String key, String value); 
}

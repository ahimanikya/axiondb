/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.commands;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.jdbc.AxionResultSet;

/**
 * Abstract base class for things like <code>DROP TABLE</code> and
 * <code>DROP SEQUENCE</code>.
 * 
 * @version  
 * @author Rodney Waldhoff 
 * @author Ahimanikya Satapathy
 */
public abstract class DropCommand extends BaseAxionCommand {

    public abstract boolean execute(Database db) throws AxionException;
    
    /** Unsupported **/
    public AxionResultSet executeQuery(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use execute.");
    }
    
    public int executeUpdate(Database database) throws AxionException {
        execute(database);
        return 0;
    }

    public String getObjectName() {
        return _objectName;
    }

    public boolean isCascade() {
        return _cascade; 
    }

    public boolean isIfExists() {
        return _ifExists; 
    }

    public void setCascade(boolean cascade) {
        _cascade = cascade;
    }
    public void setIfExists(boolean flag) {
        _ifExists = flag;
    }
    public void setObjectName(String name) {
        _objectName = name;
    }
    
    private boolean _cascade = false;
    private boolean _ifExists = false;
    private String _objectName;
}


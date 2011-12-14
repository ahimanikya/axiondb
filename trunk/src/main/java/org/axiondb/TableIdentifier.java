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

package org.axiondb;

import java.io.Serializable;

/**
 * An identifier for a table.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class TableIdentifier implements Serializable {
    public TableIdentifier() {
    }

    public TableIdentifier(String tablename) {
        _tableName = tablename;
    }

    public TableIdentifier(String tablename, String tablealias) {
        _tableName = tablename;
        _tableAlias = tablealias;
    }

    @Override
    public boolean equals(Object otherobject) {
        if (this == otherobject) {
            return true;
        }

        if (otherobject instanceof TableIdentifier) {
            TableIdentifier that = (TableIdentifier) otherobject;
            return ((null == _tableName ? null == that._tableName : _tableName.equals(that._tableName)) 
                    && (null == _tableAlias ? null == that._tableAlias : _tableAlias.equals(that._tableAlias)));
        }
        return false;
    }

    public TableIdentifier getCanonicalIdentifier() {
        return new TableIdentifier(_tableName);
    }

    public String getTableAlias() {
        return _tableAlias;
    }

    public String getTableName() {
        return _tableName;
    }

    @Override
    public int hashCode() {
        int hashCode = _hash;
        if (hashCode == 0) {
            if (null != _tableName) {
                hashCode = _tableName.hashCode();
            }
            if (null != _tableAlias) {
                hashCode ^= _tableAlias.hashCode() << 4;
            }
            _hash = hashCode;
        }
        return hashCode;
    }

    @Override
    public String toString() {
        StringBuffer result = new StringBuffer();
        result.append(_tableName);
        if (_tableAlias != null) {
            result.append(" AS ");
            result.append(_tableAlias);
        }
        return result.toString();
    }
    
    private int _hash;
    private String _tableAlias;
    private String _tableName;
    
    private static final long serialVersionUID = 8863469319713738756L;
}

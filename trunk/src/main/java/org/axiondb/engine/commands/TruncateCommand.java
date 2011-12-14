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

package org.axiondb.engine.commands;

import java.util.Iterator;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Table;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.engine.tables.TableView;
import org.axiondb.jdbc.AxionResultSet;

/**
 * A <code>TRUNCATE TABLE</code> command.
 *
 * @version  
 * @author Ahimanikya Satapathy
 */
public class TruncateCommand extends BaseAxionCommand {

    public TruncateCommand() {
    }
    
    public int executeUpdate(org.axiondb.Database db) throws AxionException {
        assertNotReadOnly(db);
        Table t = db.getTable(_tableName);
        
        if (t != null && !t.getType().equals(TableView.VIEW) 
            && !t.getType().equals(Table.SYSTEM_TABLE_TYPE)) {
            checkConstraint(db, t);
            int rowcount = t.getRowCount();
            t.truncate();
            setEffectedRowCount(rowcount);
            return rowcount;
        } else {
            throw new AxionException("Table " + _tableName + " not found.");
        }
    }

    /** Unsupported */
    public AxionResultSet executeQuery(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use executeUpdate.");
    }

    public boolean execute(Database db) throws AxionException {
        executeUpdate(db);
        return false;
    }
    
    public void setObjectName(String theTableName) {
        _tableName = theTableName;
    }
    
    protected void checkConstraint(Database db, Table table) throws AxionException {
        for(Iterator iter = table.getConstraints();iter.hasNext();) {
            Object constraint = iter.next();
            if(constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk = (ForeignKeyConstraint)constraint;
                if(fk.getParentTableName().equals(fk.getChildTableName())) {
                    // do nothing
                } else if(table.getName().equals(fk.getParentTableName())) {
                    Table childTable = db.getTable(fk.getChildTableName());
                    // someone else refering this table and has rows count > 0
                    if(childTable.getRowCount() != 0) {
                        throw new AxionException("Child table exist, can't truncate");
                    }
                }
            }
        }
    }
    
    private String _tableName;
}



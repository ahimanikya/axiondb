/*
 * 
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

package org.axiondb.engine.commands;

import java.util.Iterator;
import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Table;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.engine.tables.TableView;

/**
 * A <code>DROP TABLE</code> command. One can't drop SYSTEM_TABLE or VIEW using this
 * command. To drop view use <code>DROP VIEW viewname</code>
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class DropTableCommand extends DropCommand {

    public DropTableCommand(String tableName, boolean exists, boolean cascade) {
        setObjectName(tableName);
        setIfExists(exists);
        setCascade(cascade);
    }

    public boolean execute(Database db) throws AxionException {
        assertNotReadOnly(db);
        if (!isIfExists() || db.hasTable(getObjectName())) {
            Table t = db.getTable(getObjectName());
            if (t != null && !t.getType().equals(TableView.VIEW)
                && !t.getType().equals(Table.SYSTEM_TABLE_TYPE)) {
                checkConstraint(db, t);
                List depedentViews = db.getDependentViews(getObjectName());
                dropDepedentViews(db, depedentViews);
                db.dropTable(getObjectName());
            } else {
                throw new AxionException("No table " + getObjectName() + " found");
            }
        }
        return false;
    }

    private void dropDepedentViews(Database db, List depedentViews) throws AxionException {
        if (depedentViews.size() > 0) {
            if (isCascade()) {
                db.dropDependentViews(depedentViews);
            } else {
                throw new AxionException("Can't drop table: " + getObjectName() + " has reference in another View...");
            }
        }
    }
    
    private void checkConstraint(Database db, Table table) throws AxionException {
        for(Iterator iter = table.getConstraints();iter.hasNext();) {
            Object constraint = iter.next();
            if(constraint instanceof ForeignKeyConstraint) {
                ForeignKeyConstraint fk = (ForeignKeyConstraint)constraint;
                if(fk.getParentTableName().equals(fk.getChildTableName())) {
                    // do nothing
                } else if(table.getName().equals(fk.getChildTableName())) {
                    Table parentTable = db.getTable(fk.getParentTableName());
                    parentTable.removeConstraint(fk.getName());
                } else if(isCascade() && table.getName().equals(fk.getParentTableName())) {
                    Table childTable = db.getTable(fk.getChildTableName());
                    childTable.removeConstraint(fk.getName());
                } else { // someone else refering this table
                    throw new AxionException("Child table exist, can't drop");
                }
            }
        }
    }
    
}

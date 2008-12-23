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

import java.util.Iterator;

import org.axiondb.AxionException;
import org.axiondb.Constraint;
import org.axiondb.Database;
import org.axiondb.Table;
import org.axiondb.constraints.ForeignKeyConstraint;
import org.axiondb.constraints.UniqueConstraint;

/**
 * A <code>DROP CONSTRAINT</code> command.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class DropConstraintCommand extends ConstraintCommand {

    public DropConstraintCommand(String tableName, String constraintName, boolean cascade) {
        super(tableName);
        _cascade = cascade;
        setConstraintName(constraintName);
    }

    public String getConstraintName() {
        return _constraintName;
    }

    public void setConstraintName(String name) {
        _constraintName = name;
    }

    @Override
    public String toString() {
        StringBuffer buf = new StringBuffer();
        buf.append("ALTER TABLE ");
        buf.append(getTableName());
        buf.append(" DROP CONSTRAINT ");
        buf.append(getConstraintName());
        buf.append(_cascade ? " CASCADE " : "");
        return buf.toString();
    }

    protected void execute(Database db, Table table) throws AxionException {
        if (null == _constraintName) {
            throw new AxionException("Constraint name must not be null.");
        }

        Constraint constraint = table.removeConstraint(getConstraintName());
        if (constraint == null) {
            throw new AxionException("Constraint " + _constraintName + " not found.");
        } else if (constraint instanceof ForeignKeyConstraint) {
            handleForeignKeyConstraint(db, table, constraint);
        } else if (constraint instanceof UniqueConstraint) {
            handleUniqueConstraint(db, table, constraint);
        }
    }

    private void handleForeignKeyConstraint(Database db, Table table, Constraint constraint) throws AxionException {
        ForeignKeyConstraint fk = (ForeignKeyConstraint) constraint;
        if (fk.getParentTableName().equals(fk.getChildTableName())) {
            // do nothing
        } else if (table.getName().equals(fk.getChildTableName())) {
            Table parentTable = db.getTable(fk.getParentTableName());
            parentTable.removeConstraint(getConstraintName());
        } else {
            // Add it back since parent table does not own ForeignKeyConstraint
            table.addConstraint(constraint);
            throw new AxionException("Constraint " + _constraintName + " not found.");
        }
    }

    private void handleUniqueConstraint(Database db, Table table, Constraint constraint) throws AxionException {
        UniqueConstraint uc = (UniqueConstraint) constraint;
        for (Iterator fks = uc.getFKs(); fks.hasNext();) {
            String fkName = (String) fks.next();
            ForeignKeyConstraint fk = (ForeignKeyConstraint) table.getConstraint(fkName);
            if (fk.getParentTableName().equals(fk.getChildTableName())) {
                if (_cascade) {
                    table.removeConstraint(fkName);
                } else {
                    throw new AxionException("FK: " + fkName + " refering this PK, ca't drop constraint");
                }
            } else if (table.getName().equals(fk.getParentTableName())) {
                if (_cascade) {
                    table.removeConstraint(fkName);
                    Table childTable = db.getTable(fk.getChildTableName());
                    childTable.removeConstraint(fkName);
                } else {
                    throw new AxionException("FK: " + fkName + " refering this PK, ca't drop constraint");
                }
            }
        }
    }

    private boolean _cascade = false;
    private String _constraintName;

}


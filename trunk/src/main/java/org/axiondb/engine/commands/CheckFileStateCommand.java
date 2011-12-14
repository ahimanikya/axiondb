/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2005 Axion Development Team.  All rights reserved.
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

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.RowDecorator;
import org.axiondb.TableIdentifier;
import org.axiondb.Transaction;
import org.axiondb.engine.DiskDatabase;
import org.axiondb.engine.rowiterators.EmptyRowIterator;
import org.axiondb.engine.rowiterators.RowIteratorRowDecoratorIterator;
import org.axiondb.engine.rowiterators.SingleRowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.jdbc.AxionResultSet;
import org.axiondb.types.CharacterVaryingType;

/**
 * A <code>CHECKFILESTATE</code> query. Generates a non-empty {@link java.sql.ResultSet}
 * if the database's files are accessible, an empty one otherwise.
 * 
 * @version  
 * @author Rodney Waldhoff
 */
@SuppressWarnings("unchecked")
public class CheckFileStateCommand extends BaseAxionCommand {
    public CheckFileStateCommand() {
    }

    public boolean execute(Database db) throws AxionException {
        setResultSet(executeQuery(db));
        return (getResultSet() != null);
    }

    public AxionResultSet executeQuery(Database db) throws AxionException {
        boolean success = false;
        while (db instanceof Transaction) {
            db = ((Transaction) db).getOpenOnTransaction();
        }
        if (db instanceof DiskDatabase) {
            DiskDatabase diskdb = (DiskDatabase) db;
            File file = diskdb.getDBDirectory();
            if (!file.exists()) {
                success = false;
            } else {
                success = true;
            }
        } else {
            success = true;
        }

        if (success) {
            return createSuccessResultSet();
        }
        return createFailureResultSet();
    }

    public AxionResultSet createSuccessResultSet() {
        SimpleRow row = new SimpleRow(1);
        row.set(0, "X");
        SingleRowIterator iter = new SingleRowIterator(row);
        RowDecorator decorator = new RowDecorator(MAP);
        RowIteratorRowDecoratorIterator deciter = new RowIteratorRowDecoratorIterator(iter, decorator);
        return new AxionResultSet(deciter, SELECTED, null);
    }

    public AxionResultSet createFailureResultSet() {
        RowDecorator decorator = new RowDecorator(MAP);
        RowIteratorRowDecoratorIterator deciter = new RowIteratorRowDecoratorIterator(EmptyRowIterator.INSTANCE, decorator);
        return new AxionResultSet(deciter, SELECTED, null);
    }

    public int executeUpdate(Database database) throws AxionException {
        throw new UnsupportedOperationException("Use execute query.");
    }

    @Override
    public String toString() {
        return "CHECK FILE STATE";
    }

    private static final ColumnIdentifier[] SELECTED = new ColumnIdentifier[] { new ColumnIdentifier(new TableIdentifier(null), "DUMMY", null, new CharacterVaryingType(1))};
    private static final Map MAP = new HashMap();
    static {
        MAP.put(SELECTED[0], new Integer(0));
    }

}

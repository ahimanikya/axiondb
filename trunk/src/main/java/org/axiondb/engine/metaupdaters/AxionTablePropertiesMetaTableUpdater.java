/*
 * 
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.metaupdaters;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.Table;
import org.axiondb.engine.TransactableTableImpl;
import org.axiondb.engine.commands.DeleteCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.BaseDatabaseModificationListener;
import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.event.DatabaseModifiedEvent;
import org.axiondb.functions.FunctionIdentifier;

/**
 * Updates the <code>AXION_TABLE_PROPERTIES</code> meta table
 * 
 * @version  
 * @author Jonathan Giron
 */
public class AxionTablePropertiesMetaTableUpdater extends BaseDatabaseModificationListener implements DatabaseModificationListener {
    private static Logger _log = Logger.getLogger(AxionTablePropertiesMetaTableUpdater.class.getName());
    private Database _db = null;

    public AxionTablePropertiesMetaTableUpdater(Database db) {
        _db = db;
    }

    public void tableAdded(DatabaseModifiedEvent e) {
        Table tbl = e.getTable();
        Table propsTable = null;

        try {
            propsTable = _db.getTable("AXION_TABLE_PROPERTIES");
        } catch (AxionException ex) {
            _log.log(Level.SEVERE,"Unable to locate system table AXION_TABLE_PROPERTIES for update", ex);
        }

        List rows = createRowsForAddedTable(tbl);
        Iterator iter = rows.iterator();
        while (iter.hasNext() && propsTable != null) {
            try {
                propsTable.addRow((Row) iter.next());
            } catch (AxionException ex) {
                _log.log(Level.SEVERE,"Unable to mention table in system tables", ex);
            }
        }
    }

    private List createRowsForAddedTable(Table table) {
        List rowList = Collections.EMPTY_LIST;
        if (table instanceof TransactableTableImpl) {
            table = ((TransactableTableImpl) table).getTable();
        }
        
        if (table instanceof ExternalTable) {
            rowList = new ArrayList();
            
            final String tableName = table.getName();
            
            ExternalTable ffTable = (ExternalTable) table;
            Properties p = ffTable.getTableProperties();
            if (p != null) {
                Iterator iter = p.entrySet().iterator();
                while (iter.hasNext()) {
                    Map.Entry entry = (Map.Entry) iter.next();
                    String key = (String) entry.getKey();
                    
                    SimpleRow row = new SimpleRow(3);
                    row.set(0, tableName);
                    row.set(1, key);
                    row.set(2, entry.getValue());
                    
                    rowList.add(row);
                }
            }
        }         
        return rowList;
    }

    public void tableDropped(DatabaseModifiedEvent e) {
        FunctionIdentifier fn = new FunctionIdentifier("=");
        fn.addArgument(new ColumnIdentifier("TABLE_NAME"));
        fn.addArgument(new Literal(e.getTable().getName()));
        AxionCommand cmd = new DeleteCommand("AXION_TABLE_PROPERTIES", fn);
        try {
            cmd.execute(_db);
        } catch (AxionException ex) {
            _log.log(Level.SEVERE,"Unable to remove mention of table in system tables", ex);
        }
    }
}


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

import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.AxionCommand;
import org.axiondb.AxionException;
import org.axiondb.ColumnIdentifier;
import org.axiondb.Database;
import org.axiondb.DatabaseLink;
import org.axiondb.Literal;
import org.axiondb.Row;
import org.axiondb.engine.BaseDatabase;
import org.axiondb.engine.commands.DeleteCommand;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.BaseDatabaseModificationListener;
import org.axiondb.event.DatabaseLinkEvent;
import org.axiondb.event.DatabaseModificationListener;
import org.axiondb.functions.FunctionIdentifier;

/**
 * Updates the <code>AXION_DB_LINKS</code> meta table
 * 
 * @version  
 * @author Jonathan Giron
 */
public class AxionDBLinksMetaTableUpdater extends BaseDatabaseModificationListener implements DatabaseModificationListener {
    private static Logger _log = Logger.getLogger(AxionDBLinksMetaTableUpdater.class.getName());
    private Database _db = null;

    public AxionDBLinksMetaTableUpdater(Database db) {
        _db = db;
    }


    protected Row createRowForAddedServer(DatabaseLink server) {
        String name = server.getName();
        String url = server.getJdbcUrl();
        String userName = server.getUserName();
        
        SimpleRow row = new SimpleRow(3);
        row.set(0, name);                       // link name
        row.set(1, url);                         // JDBC URL
        row.set(2, userName);                   // user name
        return row;
    }

    public void serverAdded(DatabaseLinkEvent e) {
        Row row = createRowForAddedServer(e.getExternalDatabaseLink());
        try {
            _db.getTable(BaseDatabase.SYSTABLE_DB_LINKS).addRow(row);
        } catch (AxionException ex) {
            _log.log(Level.SEVERE,"Unable to mention database link in system tables", ex);
        }
    }

    public void serverDropped(DatabaseLinkEvent e) {
        FunctionIdentifier fn = new FunctionIdentifier("=");
        fn.addArgument(new ColumnIdentifier("LINK_NAME"));
        fn.addArgument(new Literal(e.getExternalDatabaseLink().getName()));
        AxionCommand cmd = new DeleteCommand(BaseDatabase.SYSTABLE_DB_LINKS, fn);
        try {
            cmd.execute(_db);
        } catch (AxionException ex) {
            _log.log(Level.SEVERE,"Unable to remove mention of database link in system tables", ex);
        }
    }
}


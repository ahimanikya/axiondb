/*
 * WebRowSetTableLoader.java
 *
 * Created on February 9, 2007, 10:48 AM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.axiondb.engine.tables;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.ExternalTableLoader;
import org.axiondb.Table;

/**
 *
 * @author karthikeyan s
 */
public class WebRowSetTableLoader implements ExternalTableLoader {

    public ExternalTable createExternalTable(Database database, String name) throws AxionException {
        return new WebRowSetTable(name, database);
    }

    public Table createTable(Database database, String name) throws AxionException {
        return createExternalTable(database, name);
    }
    
}

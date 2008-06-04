/*
 * WebTableLoader.java
 *
 * Created on March 18, 2007, 1:39 PM
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
public class WebTableLoader implements ExternalTableLoader {
    
    public ExternalTable createExternalTable(Database database, String name) throws AxionException {
        return new WebTable(name, database);
    }
    
    public Table createTable(Database database, String name) throws AxionException {
        return createExternalTable(database, name);
    }
    
}

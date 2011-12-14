/*
 * $Id: ExternalTable.java,v 1.9 2007/04/19 18:37:52 nilesh_apte Exp $
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

package org.axiondb;

import java.util.Properties;

/**
 * Extends Table interface to accept configuration parameters associated with connecting a
 * table to an external resource, such as a flatfile or external JDBC-compatible database.
 * <p>
 * Typically the external tables or data file are pre-existing so, we need to get more
 * meta information about the data organization so that it can load those existing data
 * file as Axion table.
 * 
 * @version $Revision: 1.9 $ $Date: 2007/04/19 18:37:52 $
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public interface ExternalTable extends Table {

    public static final String DELIMITED_TABLE_TYPE = "DELIMITED TEXT TABLE";
    public static final String FW_TABLE_TYPE = "FIXED WIDTH TEXT TABLE";
    public static final String WEBROWSET_TABLE_TYPE = "WEBROWSET TABLE";
    public static final String XML_TABLE_TYPE = "XML TABLE";
    public static final String WEB_TABLE_TYPE = "WEB TABLE";
    public static final String SPREADSHEET_TABLE_TYPE = "SPREADSHEET TABLE";
    public static final String RSS_TABLE_TYPE = "RSS TABLE";
    public static final String TAGGED_EBCDIC_TABLE_TYPE = "TAGGED EBCDIC TABLE";
    public static final String EXTERNAL_DB_TABLE_TYPE = "EXTERNAL DB TABLE";
    public static final int UNKNOWN_ROWID = -9999999;
    
    /** Property key name for catalog name */
    public static final String PROP_CATALOG = "CATALOG";

    /** Property key name for database link */
    public static final String PROP_DB = "DBLINK";

    /** Property key name for where filter */
    public static final String PROP_ORDERBY = "ORDERBY";

    /** Property key name for remote table name */
    public static final String PROP_REMOTETABLE = "REMOTETABLE";

    /** Property key name for schema name */
    public static final String PROP_SCHEMA = "SCHEMA";

    /** Property key name for where filter */
    public static final String PROP_WHERE = "WHERE";

    /** Property key representing specific table type to be built */
    public static final String PROP_LOADTYPE = "LOADTYPE"; // NOI18N
    
    /** Property key representing (optional) DB vendor name for remote table */
    public static final String PROP_VENDOR = "VENDOR"; // NOI18N
   
    public static final String COLUMNS_ARE_CASE_SENSITIVE = "COLUMNS_ARE_CASE_SENSITIVE";
    
    public static final String PROP_CREATE_IF_NOT_EXIST = "CREATE_IF_NOT_EXIST";    

    /**
     * Loads external data using the given properties table - should be called only once by
     * the table factory.
     *
     * @param prop configuration properties for this external table
     */
    boolean loadExternalTable(Properties prop) throws AxionException;

    /**
     * Gets Organization Property.
     * 
     * @return Set of organization property key names;
     */
    Properties getTableProperties();
    
    void remount() throws AxionException;
}

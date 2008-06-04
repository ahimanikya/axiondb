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

package org.axiondb;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * A Database Link that holds connection spec for a external Database <code>
 * CREATE DATABASE LINK
 * <name>(DRIVER=' <driver class name, e.g., oracle.jdbc.OracleDriver>' URL='
 * <DB-specific URL, e.g., jdbc:oracle:thin:@db1:1521:db1>' USERNAME=' <username>'
 * PASSWORD=' <password>');
 * </code>
 *
 * @version  
 * @author Ahimanikya Satapathy
 */
public class DatabaseLink {
    /**
     * Create an External Server
     */
    public DatabaseLink(String name, Properties spec) {
        _name = name;
        _connProvider = getExternalConnectionProvider();
        _connSpec = spec;

        _driverClass = spec.getProperty(ExternalConnectionProvider.PROP_DRIVERCLASS);
        _jdbcUrl = spec.getProperty(ExternalConnectionProvider.PROP_JDBCURL);
        _userName = spec.getProperty(ExternalConnectionProvider.PROP_USERNAME);
        _password = spec.getProperty(ExternalConnectionProvider.PROP_PASSWORD);

        _catalogName = spec.getProperty(ExternalConnectionProvider.PROP_CATALOG);
        _schemaName = spec.getProperty(ExternalConnectionProvider.PROP_SCHEMA);

        if ((_connProvider == null) &&
            (_driverClass == null || _driverClass.trim().length() == 0 ||
            _jdbcUrl == null || _jdbcUrl.trim().length() == 0 ||
            _userName == null || _userName.trim().length() == 0 ||
            _password == null) ) {
            throw new IllegalArgumentException("Missing required connection properties.");
        }
    }

    private ExternalConnectionProvider getExternalConnectionProvider()  {
        Object connectionProvider = null;
        String externalConnectionProvider =
          System.getProperty(ExternalConnectionProvider.EXTERNAL_CONNECTION_PROVIDER_PROPERTY_NAME);          
        if(externalConnectionProvider != null && !externalConnectionProvider.trim().equals("")) {
            try {
                connectionProvider = Thread.currentThread().getContextClassLoader()
                    .loadClass(externalConnectionProvider).newInstance();
            } catch (Exception e) {
                // ignore for now
            }
        }
        return (ExternalConnectionProvider)connectionProvider;
    }

    public Connection getConnection() throws AxionException {
        Connection conn = null;
        try {
            if(_connProvider == null) {
                Class.forName(_driverClass);
                conn = DriverManager.getConnection(_jdbcUrl, _userName, _password);
            } else {
                conn = _connProvider.getConnection(_connSpec);
            }
            conn.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Could not locate class for driver: " + _driverClass);
        } catch (SQLException e) {
            throw new AxionException("Could not connect to database for URL: " + _jdbcUrl, e);
        }
        return conn;
    }

    /**
     * Get the name of this database link.
     */
    public String getName() {
        return _name;
    }

    /**
     * @return Returns the _catalogName.
     */
    public String getCatalogName() {
        return _catalogName;
    }

    /**
     * @return Returns the _schemaName.
     */
    public String getSchemaName() {
        return _schemaName;
    }

    /**
     * @return JDBC URL to use in connecting to the associated server
     */
    public String getJdbcUrl() {
        return _jdbcUrl;
    }

    /**
     * @return user name to authenticate against in connecting to associated server
     */
    public String getUserName() {
        return _userName;
    }
    
    /**
     * @return connection properties
     */    
    public Properties getProperties() {
        return _connSpec;
    }

    private String _name = null;
    private String _driverClass = null;
    private String _jdbcUrl;
    private String _userName;
    private String _password;
    private String _catalogName;
    private String _schemaName;
    private Properties _connSpec;
    private ExternalConnectionProvider _connProvider;

}

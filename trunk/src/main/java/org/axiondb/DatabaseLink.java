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

import java.io.File;
import java.io.FileFilter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

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
 * @author Jawed
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
                _password == null)) {
            throw new IllegalArgumentException("Missing required connection properties.");
        }
    }

    private ExternalConnectionProvider getExternalConnectionProvider() {
        Object connectionProvider = null;
        String externalConnectionProvider =
                System.getProperty(ExternalConnectionProvider.EXTERNAL_CONNECTION_PROVIDER_PROPERTY_NAME);
        if (externalConnectionProvider != null && !externalConnectionProvider.trim().equals("")) {
            try {
                connectionProvider = Thread.currentThread().getContextClassLoader().loadClass(externalConnectionProvider).newInstance();
            } catch (Exception e) {
            // ignore for now
            }
        }
        return (ExternalConnectionProvider) connectionProvider;
    }

    public Connection getConnection() throws AxionException {
        Connection conn = null;
        try {
            if (_connProvider != null) {
                conn = _connProvider.getConnection(_connSpec);

            } else {
                String nbHome = System.getProperty("netbeans.home");
                if (nbHome != null) {
                    conn = getExternalDBConnection(_driverClass, _userName, _password, _jdbcUrl);
                } else {
                    Class.forName(_driverClass);
                    conn = DriverManager.getConnection(_jdbcUrl, _userName, _password);
                }

            }
            conn.setAutoCommit(false);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Could not locate class for driver: " + _driverClass);
        } catch (SQLException e) {
            throw new AxionException("Could not connect to database for URL: " + _jdbcUrl, e);
        }
        return conn;
    }

    public URL[] getDriverUrls(String driverClass) {
        URL[] urls;
        String drvClass = driverClass.replace(".", "_") + ".xml";
        String nbUsrDir = System.getProperty("netbeans.user");
        String nbHomeDir = getNetbeansHome();
        if ((nbUsrDir.length() == 0) && (nbHomeDir.length() == 0)) {
            Logger.getLogger(DatabaseLink.class.getName()).info(" Neatbeans user/home directory doesn't exist");
            return null;
        }
        drvClass = nbUsrDir + File.separator + "config" + File.separator + "Databases" + File.separator + "JDBCDrivers" + File.separator + drvClass;
        File xmlConfig = new File(drvClass);

        if (xmlConfig.exists()) {
            try {
                DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
                DocumentBuilder db = dbf.newDocumentBuilder();
                Document doc = db.parse(xmlConfig);
                doc.getDocumentElement().normalize();

                List list = new ArrayList();
                NodeList nodeLst = doc.getElementsByTagName("urls");
                Node node = nodeLst.item(0);
                NodeList cList = node.getChildNodes();
                for (int i = 0; i < cList.getLength(); i++) {
                    Node cNode = cList.item(i);
                    if (cNode.getNodeType() == Node.ELEMENT_NODE) {
                        Element elem = (Element) cNode;
                        String attrValue = elem.getAttribute("value");
                        list.add(attrValue);
                    }
                }
                urls = new URL[list.size()];
                int j = 0;
                try {
                    for (Iterator iter = list.iterator(); iter.hasNext(); j++) {
                        urls[j] = new URL((String) iter.next());
                    }
                } catch (MalformedURLException ex) {
                    Logger.getLogger(DatabaseLink.class.getName()).log(Level.SEVERE, null, ex);
                }
                return urls;
            } catch (Exception ex) {
                Logger.getLogger(DatabaseLink.class.getName()).log(Level.SEVERE, null, ex);
            }
        } else {
            try {
                String ide = getClusters(new File(nbHomeDir), "ide");
                String loc = nbHomeDir + File.separator + ide + File.separator + "modules" + File.separator + "ext" + File.separator;
                xmlConfig = new File(loc);
                File[] jars = xmlConfig.listFiles(new Filter());  
                urls = new URL[jars.length];
                for(int i = 0; i<jars.length; i++){
                    urls[i] = new URL(new String("file:" + File.separator + jars[i].getAbsolutePath()));
                }
                return urls;
            } catch (MalformedURLException ex) {
                Logger.getLogger(DatabaseLink.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        return null;
    }

    private class Filter implements FileFilter {
        public boolean accept(File file) {
            return file.getName().contains("mysql");
        }
    }
    
    public static String getClusters(File dir, String str) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                if (children[i].trim().toLowerCase().startsWith(str)) {
                    return children[i];
                }
            }
        }
        return null;
    }
    
    public Connection getExternalDBConnection(String driverClass, String username, String password, String url) {
        Driver drv = null;
        Connection con = null;
        try {
            Properties prop = new Properties();
            prop.setProperty("user", username);
            prop.setProperty("password", password);

            URL[] driverURLs = getDriverUrls(driverClass);
            if (driverURLs != null) {
                URLClassLoader urlLoader = new URLClassLoader(driverURLs);
                Class c = Class.forName(driverClass, true, urlLoader);
                drv = (Driver)c.newInstance();
                con = drv.connect(url, prop);
            }
        } catch (Exception ex) {
            Logger.getLogger(DatabaseLink.class.getName()).log(Level.SEVERE, null, ex);
        }
        return con;
    }

    private static String getNetbeansHome() {
        String netbeansHome = System.getProperty("netbeans.home");
        if (!netbeansHome.endsWith("netbeans")) {
            File f = new File(netbeansHome);
            netbeansHome = f.getParentFile().getAbsolutePath();
        }
        return netbeansHome;
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

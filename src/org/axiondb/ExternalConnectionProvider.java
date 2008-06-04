/*
 * 
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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
import java.util.Properties;

/**
 * Provides External Database connection for a given DatabaseLink.
 *
 * @author Ahimanikya Satapathy
 * @version  
 */
public interface ExternalConnectionProvider  {
    /** System Property key name for connection provider class name */
    public static final String EXTERNAL_CONNECTION_PROVIDER_PROPERTY_NAME = "org.axiondb.external.connection.provider";
    /** Property key name for JDBC driver class */
    public static final String PROP_DRIVERCLASS = "DRIVER";
    /** Property key name for JDBC URL string */
    public static final String PROP_JDBCURL = "URL";
    /** Property key name for user name */
    public static final String PROP_USERNAME = "USERNAME";
    /** Property key name for user password */
    public static final String PROP_PASSWORD = "PASSWORD";
    /** Property key name for Catalog Name */
    public static final String PROP_CATALOG = "CATALOG";
    /** Property key name for SchemaName */
    public static final String PROP_SCHEMA = "SCHEMA";


    Connection getConnection(Properties spec) throws AxionException;
}

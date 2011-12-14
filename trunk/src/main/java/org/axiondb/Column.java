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

package org.axiondb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * Describes a column within a {@link Table}.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class Column implements Serializable {

    /**
     * Key for setting and retrieving the sqlType in this column's configuration
     */
    public static final String COLUMN_SQL_TYPE_CONFIG_KEY = "sqlType";

    /**
     * Key for setting and retrieving the {@link DataType}in this column's configuration
     */
    public static final String DATA_TYPE_CONFIG_KEY = "type";

    /**
     * Key for setting and retrieving the {@link Selectable default value}in this
     * column's configuration
     */
    public static final String DEFAULT_VALUE_CONFIG_KEY = "defaultValue";

    /**
     * Key for setting and retrieving the name in this column's configuration
     */
    public static final String NAME_CONFIG_KEY = "name";

    /**
     * Key for setting and retrieving the Identity column generation type
     */
    public static final String IDENTITY_GENERATION_TYPE = "identityType";

    public static final String GENERATED_ALWAYS = "always";
    public static final String GENERATED_BY_DEFAULT = "default";

    /**
     * Key for setting and retrieving generated column expression
     */
    public static final String GENERATED_COLUMN_TYPE = "generatedCol";

    /**
     * Create column with the given <i>name </i> and <i>type </i>.
     * 
     * @param name the name of this column, which MUST NOT be <code>null</code>
     * @param type the {@link DataType}of this column, which MUST NOT be
     *        <code>null</code>
     * @throws NullPointerException if either parameter is <code>null</code>
     */
    public Column(String name, DataType type) throws NullPointerException {
        this(name, type, null);
    }

    /**
     * Create column with the given <i>name </i> and <i>type </i>.
     * 
     * @param name the name of this column, which MUST NOT be <code>null</code>
     * @param type the {@link DataType}of this column, which MUST NOT be
     *        <code>null</code>
     * @param config name-value pairs that configure this column
     * @param defaultValue the {@link Selectable default value}for this column, which may
     *        be <code>null</code>
     * @throws NullPointerException if either name or type is <code>null</code>
     */
    @SuppressWarnings("unchecked")
    public Column(String name, DataType type, Selectable defaultValue) throws NullPointerException {
        if (null == name) {
            throw new NullPointerException("name parameter must not be null");
        }
        if (null == type) {
            throw new NullPointerException("type parameter must not be null");
        }
        _config = new HashMap();
        getConfiguration().put(NAME_CONFIG_KEY, name);
        getConfiguration().put(DATA_TYPE_CONFIG_KEY, type);
        getConfiguration().put(DEFAULT_VALUE_CONFIG_KEY, defaultValue);
    }

    /** Two {@link Column}s are equal if they have the same name. */
    @Override
    public boolean equals(Object that) {
        if (that instanceof Column) {
            Column col = (Column) that;
            return getName().equals(col.getName());
        }
        return false;
    }

    public final Map getConfiguration() {
        return _config;
    }

    /**
     * Get the {@link DataType}of this column.
     */
    public final DataType getDataType() {
        return (DataType) getConfiguration().get(DATA_TYPE_CONFIG_KEY);
    }

    public final Selectable getDefault() {
        return (Selectable) getConfiguration().get(DEFAULT_VALUE_CONFIG_KEY);
    }

    /**
     * Get the name of this column.
     */
    public final String getName() {
        return (String) getConfiguration().get(NAME_CONFIG_KEY);
    }

    public int getScale() {
        return getDataType().getScale();
    }

    public int getSize() {
        return getDataType().getPrecision();
    }

    public String getSqlType() {
        return (String) getConfiguration().get(COLUMN_SQL_TYPE_CONFIG_KEY);
    }

    public boolean hasDefault() {
        return null != getDefault();
    }

    public boolean isIdentityColumn() {
        return null != getIdentityType();
    }

    public boolean isGeneratedAlways() {
        return GENERATED_ALWAYS.equals(getIdentityType());
    }

    public boolean isGeneratedByDefault() {
        return GENERATED_BY_DEFAULT.equals(getIdentityType());
    }

    public boolean isDerivedColumn() {
        return GENERATED_ALWAYS.equals(getConfiguration().get(GENERATED_COLUMN_TYPE));
    }

    @SuppressWarnings("unchecked")
    public void setGeneratedColType(String type) {
        getConfiguration().put(GENERATED_COLUMN_TYPE, type);
    }

    public String getGeneratedColType() {
        return (String) getConfiguration().get(GENERATED_COLUMN_TYPE);
    }

    public final String getIdentityType() {
        return (String) getConfiguration().get(IDENTITY_GENERATION_TYPE);
    }

    @SuppressWarnings("unchecked")
    public void setIdentityType(String type) {
        getConfiguration().put(IDENTITY_GENERATION_TYPE, type);
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @SuppressWarnings("unchecked")
    public void setSqlType(String type) {
        getConfiguration().put(COLUMN_SQL_TYPE_CONFIG_KEY, type);
    }

    @Override
    public String toString() {
        return getName();
    }

    private Map _config;
    private static final long serialVersionUID = -9163914166152422736L;
}

/*
 * 
 * =======================================================================
 * Copyright (c) 2003-2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.engine.indexes;

import java.io.File;
import java.io.IOException;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.DataType;
import org.axiondb.IndexLoader;
import org.axiondb.engine.StringBTreeIndexLoader;
import org.axiondb.util.NullObject;
import org.axiondb.util.ObjectBTree;
import org.axiondb.util.StringBTree;

/**
 * @version  
 * @author Rodney Waldhoff
 */
public class StringBTreeIndex extends ObjectBTreeIndex {

    public StringBTreeIndex(String name, Column column, boolean unique) throws AxionException {
        this(name, column, unique, 1000);
    }

    public StringBTreeIndex(String name, Column column, boolean unique, int minimizationFactor) throws AxionException {
        this(name, column, unique, minimizationFactor, null);
    }

    public StringBTreeIndex(String name, Column column, boolean unique, int minimizationFactor, File dataDirectory) throws AxionException {
        super(name, column, unique, dataDirectory);
        _minimizationFactor = minimizationFactor;
    }

    public IndexLoader getIndexLoader() {
        return LOADER;
    }

    public int getMinimizationFactor() {
        return _minimizationFactor;
    }

    protected ObjectBTree createTree(File dataDirectory, String name, int minimizationFactor, DataType dataType) throws IOException,
            ClassNotFoundException {
        return new StringBTree(dataDirectory, name, minimizationFactor, dataType);
    }

    protected Object getNullKey() {
        return NullObject.INSTANCE.toString();
    }
    
    private static final IndexLoader LOADER = new StringBTreeIndexLoader();
}

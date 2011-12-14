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

package org.axiondb.engine.indexes;

import java.io.File;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Function;
import org.axiondb.Index;
import org.axiondb.functions.EqualFunction;
import org.axiondb.functions.GreaterThanFunction;
import org.axiondb.functions.GreaterThanOrEqualFunction;
import org.axiondb.functions.IsNotNullFunction;
import org.axiondb.functions.IsNullFunction;
import org.axiondb.functions.LessThanFunction;
import org.axiondb.functions.LessThanOrEqualFunction;

/**
 * Abstract base implementation for B-Tree based {@link Index indices}.
 * 
 * @version  
 * @author Dave Pekarek Krohn
 */
public abstract class BaseBTreeIndex extends BaseIndex implements Index {

    public BaseBTreeIndex(String name, Column column, boolean unique) {
        super(name, column, unique);
    }

    public String getType() {
        return Index.BTREE;
    }

    public void save(File dataDirectory) throws AxionException {
        getIndexLoader().saveIndex(this, dataDirectory);
    }

    public void saveAfterTruncate(File dataDirectory) throws AxionException {
        getIndexLoader().saveIndexAfterTruncate(this, dataDirectory);
    }

    public boolean supportsFunction(Function fn) {
        if (fn instanceof EqualFunction) {
            if (isUnique()) {
                return true;
            }
            return getIndexedColumn().getDataType().supportsSuccessor();
        } else if (fn instanceof LessThanFunction) {
            return true;
        } else if (fn instanceof LessThanOrEqualFunction) {
            return getIndexedColumn().getDataType().supportsSuccessor();
        } else if (fn instanceof GreaterThanFunction) {
            return true;
        } else if (fn instanceof GreaterThanOrEqualFunction) {
            return getIndexedColumn().getDataType().supportsSuccessor();
        } else if (fn instanceof IsNotNullFunction) {
            return true;
        } else if (fn instanceof IsNullFunction) {
            return true;
        } else {
            return false;
        }
    }

}

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

package org.axiondb.engine;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.collections.primitives.IntList;
import org.axiondb.Column;
import org.axiondb.Index;
import org.axiondb.engine.indexes.BaseArrayIndex;
import org.axiondb.engine.indexes.BaseArrayIndexLoader;
import org.axiondb.engine.indexes.ObjectArrayIndex;

/**
 * An {@link IndexLoader} for {@link ObjectArrayIndex}.
 *
 * @version  
 * @author Rodney Waldhoff
 */
public class ObjectArrayIndexLoader extends BaseArrayIndexLoader {
    public ObjectArrayIndexLoader() {
    }

    @SuppressWarnings("unchecked")
    protected Object readKeys(ObjectInputStream in) throws IOException, ClassNotFoundException {
        int keysize = in.readInt();      
        ArrayList keys = new ArrayList(keysize);
        for(int i=0;i<keysize;i++) {
            keys.add(in.readObject());
        }
        return keys;
    }

    protected Index makeIndex(String name, Column col, boolean unique, Object keys, IntList values) {
        return new ObjectArrayIndex(name,col,unique,(ArrayList)keys,values);
    }

    protected void writeKeys(ObjectOutputStream out, BaseArrayIndex baseindex) throws IOException {
        ObjectArrayIndex index = (ObjectArrayIndex)baseindex;
        List keylist = index.getKeyList();
        out.writeInt(keylist.size());
        for(int i=0,I=keylist.size();i<I;i++) {
            out.writeObject(keylist.get(i));
        }
    }

}


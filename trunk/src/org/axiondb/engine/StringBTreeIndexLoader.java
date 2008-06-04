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

package org.axiondb.engine;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.axiondb.AxionException;
import org.axiondb.Index;
import org.axiondb.Table;
import org.axiondb.engine.indexes.ObjectBTreeIndex;
import org.axiondb.engine.indexes.StringBTreeIndex;
import org.axiondb.io.AxionFileSystem;

/**
 * @version  
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public class StringBTreeIndexLoader extends ObjectBTreeIndexLoader {
    public StringBTreeIndexLoader() {
    }

    @Override
    public Index loadIndex(Table table, File dataDirectory) throws AxionException {
        ObjectInputStream in = null;
        AxionFileSystem fs = new AxionFileSystem();
        try {
            String name = dataDirectory.getName();
            File file = new File(dataDirectory, name + ".data");
            in = fs.openObjectInputSteam(file);

            int ver = in.readInt(); // read version number
            if (ver != VERSION_NUMBER) {
                throw new AxionException("Can't parse data file " + file + " for index " + name + ", unrecognized data file version " + ver);
            }

            String col = in.readUTF(); // read column name
            boolean unique = in.readBoolean(); // read unique flag
            int minimizationFactor = in.readInt(); // read minimization factor

            return new StringBTreeIndex(name, table.getColumn(col), unique, minimizationFactor, dataDirectory);
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            fs.closeInputStream(in);
        }
    }

    @Override
    public void saveIndex(Index ndx, File dataDirectory) throws AxionException {
        ObjectBTreeIndex index = (ObjectBTreeIndex) ndx;
        ObjectOutputStream out = null;
        try {
            String name = index.getName();
            File file = new File(dataDirectory, name + ".data");
            out = FS.createObjectOutputSteam(file);
            out.writeInt(VERSION_NUMBER); // write version number
            out.writeUTF(index.getIndexedColumn().getName()); // write column name
            out.writeBoolean(index.isUnique()); // write unique flag
            out.writeInt(((StringBTreeIndex) index).getMinimizationFactor()); // write minimization factor
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            FS.closeOutputStream(out);
        }
        save(ndx, dataDirectory);
    }

    private static final int VERSION_NUMBER = 2;
}

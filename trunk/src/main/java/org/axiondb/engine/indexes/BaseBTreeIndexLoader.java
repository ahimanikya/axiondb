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
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Index;
import org.axiondb.IndexLoader;
import org.axiondb.Table;
import org.axiondb.io.AxionFileSystem;

/**
 * @version  
 * @author Dave Pekarek Krohn
 * @author Ahimanikya Satapathy
 */
public abstract class BaseBTreeIndexLoader implements IndexLoader {
    protected static AxionFileSystem FS = new AxionFileSystem();
    
    public BaseBTreeIndexLoader() {
    }
    
    public Index loadIndex(Table table, File dataDirectory) throws AxionException {
        ObjectInputStream in = null;
        try {
            String name = dataDirectory.getName();
            File file = new File(dataDirectory, name + ".DATA");
            in = FS.openObjectInputSteam(file);

            int ver = in.readInt(); // read version number
            if (ver != 1) {
                throw new AxionException("Can't parse data file " + file + " for index " + name + ", unrecognized data file version " + ver);
            }

            String col = in.readUTF(); // read column name
            boolean unique = in.readBoolean(); // read unique flag
            return makeIndex(name, table.getColumn(col), unique, dataDirectory);
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            FS.closeInputStream(in);
        }
    }

    public void saveIndex(Index index, File dataDirectory) throws AxionException {
        ObjectOutputStream out = null;
        try {
            String name = index.getName();
            File file = new File(dataDirectory, name + ".DATA");
            out = FS.createObjectOutputSteam(file);
            out.writeInt(1); // write version number
            out.writeUTF(index.getIndexedColumn().getName()); // write column name
            out.writeBoolean(index.isUnique()); // write unique flag
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            FS.closeOutputStream(out);
        }
        
        save(index, dataDirectory);
    }

    public abstract void saveIndexAfterTruncate(Index ndx, File dataDirectory) throws AxionException;
    
    public abstract void save(Index ndx, File dataDirectory) throws AxionException;

    protected abstract Index makeIndex(String name, Column col, boolean unique, File dataDirectory) throws AxionException;

}

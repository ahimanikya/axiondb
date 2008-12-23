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

package org.axiondb.engine.tables;

import java.io.File;
import java.io.IOException;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Row;
import org.axiondb.Table;
import org.axiondb.engine.DiskTableFactory;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.io.FileUtil;

/**
 * A disk-resident {@link Table}.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public final class DiskTable extends BaseDiskTable {
    
    //------------------------------------------------------------- Constructors

    public DiskTable(String name, Database db) throws AxionException {
        super(name, db, new DiskTableFactory());
        createOrLoadDataFile();
        //in case the DB has been moved, we need to update the lobDir
        _lobDir = new File(getRootDir(), LOBS_DIR_NAME);
        notifyColumnsOfNewLobDir(_lobDir);
    }

    //------------------------------------------------------------------ Public

    public GlomLobsHelper getGlomLobsHelper() {
        return new GlomLobsHelper(this);
    }

    /** @deprecated use GlomLobsHelper instead */
    public void glomLobs() throws Exception {
        getGlomLobsHelper().glomLobs();
    }

    protected File getDataFile() {
        if (null == _dataFile) {
            _dataFile = new File(getRootDir(), getName() + "." + getDefaultDataFileExtension());
        }
        return _dataFile;
    }

    protected File getLobDir() {
        return _lobDir;
    }

    protected Row getRowByOffset(int idToAssign, long ptr) throws AxionException {
        BufferedDataInputStream file = getInputStream();
        return getRowByOffset(idToAssign, ptr, file);
    }

    protected void initFiles(File basedir, boolean datafilesonly) throws AxionException {
        super.initFiles(basedir, datafilesonly);
        _lobDir = new File(getRootDir(), LOBS_DIR_NAME);
        notifyColumnsOfNewLobDir(_lobDir);
    }

    protected void reloadFilesAfterTruncate() throws AxionException {
        _freeIds = new ArrayIntList();
        writeFridFile();

        // Create zero-record data file if necessary.
        try {
            if (!isReadOnly()) {
                getDataFile().createNewFile();
            }
        } catch (IOException e) {
            throw new AxionException("Unable to create data file \"" + getDataFile() + "\".", e);
        }

        initFiles(getDataFile(), true);
        initializeRowCount();
        resetLobColumns();
    }

    protected synchronized void renameTableFiles(String oldName, String name) {
        super.renameTableFiles(oldName, name);
        FileUtil.renameFile(getRootDir(), oldName, name, "." + getDefaultDataFileExtension());
    }

    protected void writeRow(BufferedDataOutputStream out, Row row) throws AxionException {
        try {
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                getColumn(i).getDataType().write(row.get(i), out);
            }
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }

    private Row getRowByOffset(int idToAssign, long ptr, BufferedDataInputStream data) throws AxionException {
        try {
            Row row = new SimpleRow(idToAssign, getColumnCount());
            synchronized (data) {
                data.seek(ptr);
                for (int i = 0, I = getColumnCount(); i < I; i++) {
                    row.set(i, getColumn(i).getDataType().read(data));
                }
            }
            return row;
        } catch (IOException e) {
            throw new AxionException("IOException in getRowByOffset while reading row" + " for table " + getName() + " at position " + ptr
                + " in data file " + getDataFile(), e);
        }
    }
    
    /** The directory in which my LOB data are stored. */
    private File _lobDir = null;
    private static final String LOBS_DIR_NAME = "LOBS";
}

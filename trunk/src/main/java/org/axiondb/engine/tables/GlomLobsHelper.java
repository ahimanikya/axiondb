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

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.SyncFailedException;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Row;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.types.FileLobLocator;
import org.axiondb.types.FileOffsetLobLocator;
import org.axiondb.types.FileOffsetLobLocatorFactory;
import org.axiondb.types.LOBType;
import org.axiondb.types.LobLocator;

/**
 * An Utility to glom lobs.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 */
public final class GlomLobsHelper {

    public GlomLobsHelper(DiskTable table) {
        _table = table;
    }

    public void glomLobs() throws Exception {
        // the glommed .data file
        BufferedDataOutputStream gdata = BaseDiskTable.FS.createBufferedDOS(new File(getTable().getRootDir(), getTable().getName() + ".DATA.GLOM"));
        // the glommed .pidx file
        File gpidxfile = getTable().getTableFile(".PIDX.GLOM");

        BufferedDataOutputStream[] glom = createGlommedLobFiles();
        AxionFileSystem.PidxList gpidx = BaseDiskTable.FS.newPidxList(gpidxfile, false);

        // FOR EACH ROW
        for (int i = 0, I = getTable().getPidxList().size();  i < I; i++) {
            long oldoffset = getTable().getPidxList().get(i);
            if (oldoffset == BaseDiskTable.INVALID_OFFSET) {
                gpidx.add(BaseDiskTable.INVALID_OFFSET);
            } else {
                SimpleRow grow = glomRow(glom, i, oldoffset);
                gpidx.add(gdata.getPos());
                writeGlommedRow(gdata, glom, grow);
            }
        }

        closeFiles(glom);

        // CLOSE OUT THE GLOMMED .DATA and .PIDX FILE
        gdata.close();
        gpidx.close();
    }

    private void closeFiles(BufferedDataOutputStream[] glom) throws IOException {
        for (int i = 0; i < glom.length; i++) {
            if (glom[i] != null) {
                glom[i].close();
            }
        }
    }

    private BufferedDataOutputStream[] createGlommedLobFiles() throws AxionException {
        BufferedDataOutputStream[] glom = new BufferedDataOutputStream[getTable().getColumnCount()];
        for (int i = 0; i < glom.length; i++) {
            Column col = getTable().getColumn(i);
            if (col.getDataType() instanceof LOBType) {
                glom[i] = BaseDiskTable.FS.createBufferedDOS(new File(getTable().getLobDir(), col.getName() + ".GLOM"));
            } else {
                glom[i] = null;
            }
        }
        return glom;
    }

    private void glomLobColumn(BufferedDataOutputStream[] glom, Row row, SimpleRow grow, int k) throws AxionException, IOException,
            FileNotFoundException, SyncFailedException {
        if (null != glom[k]) {
            // GET THE OLD FILE
            Column col = getTable().getColumn(k);
            FileLobLocator loc = (FileLobLocator) (col.getDataType().convert(row.get(k)));
            if (loc != null) {
                File oldfile = loc.getFile(((LOBType) (col.getDataType())).getLobDir());
                // WRITE IT TO THE NEW FILE
                long offset = glom[k].getPos();
                int length = 0;
                InputStream in = new BufferedInputStream(new FileInputStream(oldfile));
                for (int b = in.read(); b != -1; b = in.read()) {
                    glom[k].write(b);
                    length++;
                }
                in.close();

                // SET THE LOB LOCATOR FOR THAT
                FileOffsetLobLocator gloc = new FileOffsetLobLocator(offset, length);
                grow.set(k, gloc);
            } else {
                grow.set(k, null);
            }
        }
    }

    private SimpleRow glomRow(BufferedDataOutputStream[] glom, int i, long oldoffset) throws AxionException, IOException, FileNotFoundException,
            SyncFailedException {
        Row row = getTable().getRowByOffset(i, oldoffset);
        SimpleRow grow = new SimpleRow(row);
        // FOR EACH COLUMN
        for (int k = 0; k < glom.length; k++) {
            // FOR EACH LOB COLUMN
            glomLobColumn(glom, row, grow, k);
        }
        return grow;
    }

    private void writeGlommedRow(BufferedDataOutputStream gdata, BufferedDataOutputStream[] glom, SimpleRow grow) throws IOException {
        for (int j = 0, J = getTable().getColumnCount(); j < J; j++) {
            if (glom[j] != null) {
                gdata.writeBoolean(true);
                FILE_OFFSET_LOB_LOCATOR_FACTORY.write((LobLocator) (grow.get(j)), gdata);
            } else {
                getTable().getColumn(j).getDataType().write(grow.get(j), gdata);
            }
        }
    }

    private DiskTable getTable() {
        return _table;
    }

    private static final FileOffsetLobLocatorFactory FILE_OFFSET_LOB_LOCATOR_FACTORY = new FileOffsetLobLocatorFactory();
    private DiskTable _table = null;
}

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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.apache.commons.collections.primitives.IntList;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Constraint;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.Index;
import org.axiondb.IndexLoader;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.Sequence;
import org.axiondb.TableFactory;
import org.axiondb.engine.rowiterators.BaseRowIterator;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.io.FileUtil;
import org.axiondb.types.LOBType;
import org.axiondb.util.ExceptionConverter;

/**
 * Abstract base disk-resident implementation of {@link Table}.
 * <code>BaseDiskTable</code> manages the column meta-data for a disk-based table.
 *
 * @version
 * @author Chuck Burdick
 * @author Rodney Waldhoff
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 */
public abstract class BaseDiskTable extends BaseTable {

    //------------------------------------------------------------- Constructors
    public BaseDiskTable(String name, Database db, TableFactory factory) throws AxionException {
        super(name);
        _dbdir = db.getDBDirectory();
        _readOnly = db.isReadOnly();
        createOrLoadTableFiles(name, db, factory);
    }

    //  ----------------------------------------------------------------- public
    @Override
    public void addColumn(Column col) throws AxionException {
        addColumn(col, true);
    }

    public void addColumn(Column col, boolean metaUpdateNeeded) throws AxionException {
        resetLobColumn(col);
        super.addColumn(col);
        if (metaUpdateNeeded) {
            writeMetaFile();
        }
    }

    @Override
    public void addConstraint(Constraint constraint) throws AxionException {
        super.addConstraint(constraint);
        writeMetaFile();
    }

    public void applyDeletes(IntCollection rowIds) throws AxionException {
        synchronized (this) {
            applyDeletesToIndices(rowIds);
            applyDeletesToRows(rowIds);
        }
    }

    public void applyInserts(RowCollection rows) throws AxionException {
        synchronized (this) {
            // apply to the indices one at a time, as its more memory-friendly
            for (Iterator indexIter = getIndices(); indexIter.hasNext();) {
                Index index = (Index) (indexIter.next());
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row row = iter.next();
                    RowEvent event = new RowInsertedEvent(this, null, row);
                    index.rowInserted(event);
                }
                saveIndex(index);
            }
            applyInsertsToRows(rows.rowIterator());
        }
    }

    public void applyUpdates(RowCollection rows) throws AxionException {
        synchronized (this) {
            applyUpdatesToIndices(rows);
            applyUpdatesToRows(rows.rowIterator());
        }
    }

    @Override
    public void checkpoint() throws AxionException {
        super.checkpoint();
        DataOutputStream out = null;
        try {
            if (getSequence() != null) {
                File seqFile = getTableFile(SEQ_FILE_EXT);
                out = FS.createDataOutputSteam(seqFile);
                getSequence().write(out);
            }
        } catch (IOException e) {
            String msg = "Unable to persist sequence file";
            throw new AxionException(msg);
        } finally {
            FS.closeOutputStream(out);
        }
    }

    @Override
    public void drop() throws AxionException {
        closeFiles();
        if (!FileUtil.delete(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table " + getName());
        }
    }

    public void freeRowId(int id) {
        if (_freeIdPos >= 0 && id == _freeIds.get(_freeIdPos)) {
            _freeIdPos--;
        } else if (_nextFreeId > getPidxList().size() - 1) {
            _nextFreeId--;
        }
    }

    public int getNextRowId() {
        if (_freeIds.isEmpty() || _freeIdPos >= _freeIds.size() - 1) {
            return _nextFreeId = (_nextFreeId == -1 ? getPidxList().size() : _nextFreeId + 1);
        } else {
            return _freeIds.get(++_freeIdPos);
        }
    }

    public Row getRow(int id) throws AxionException {
        long ptr = getPidxList().get(id);
        Row row = getRowByOffset(id, ptr);
        return row;
    }

    /** Migrate from older version to newer version for this table */
    @SuppressWarnings(value = "unchecked")
    public void migrate(Database db) throws AxionException {
        File metaFile = getTableFile(META_FILE_EXT);
        if (!metaFile.exists()) {
            return;
        }

        int version = CURRENT_META_VERSION;
        ObjectInputStream in = null;
        try {
            in = FS.openObjectInputSteam(metaFile);
            // read version number
            version = in.readInt();
            _log.log(Level.FINE, "Version number for " + getName() + " in migrate() is " + version);

            if (version < 0 || version > CURRENT_META_VERSION) {
                throw new AxionException("Unrecognized version " + version);
            }

            if (version == 0) {
                parseV0MetaFile(in);
            } else {
                parseV1MetaFile(in, db);
            }
            parseTableProperties(in);
        } catch (ClassNotFoundException e) {
            throw new AxionException("Unable to parse meta file " + metaFile + " for table " + getName(), e);
        } catch (IOException e) {
            throw new AxionException("Unable to parse meta file " + metaFile + " for table " + getName(), e);
        } finally {
            FS.closeInputStream(in);
        }

        if (version > 1 && version < 4 ) {
            _pidx = FS.parseLongPidx(getTableFile(PIDX_FILE_EXT), _readOnly);
        }

        if (version < 3) {
            // change col name to upper if required
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                Column col = getColumn(i);
                col.getConfiguration().put(Column.NAME_CONFIG_KEY, col.getName().toUpperCase());
            }
        }

        if (version != CURRENT_META_VERSION) {
            writeMetaFile(); // migrating from older meta type, so update meta file
        }
    }

    public int getRowCount() {
        return _rowCount;
    }

    public void populateIndex(Index index) throws AxionException {
        for (int i = 0, I = getPidxList().size(); i < I; i++) {
            long ptr = getPidxList().get(i);
            if (ptr != INVALID_OFFSET) {
                index.rowInserted(new RowInsertedEvent(this, null, getRowByOffset(i, ptr)));
            }
        }

        File indexDir = new File(_indexRootDir, index.getName());
        if (!indexDir.exists()) {
            indexDir.mkdirs();
        }
        File typefile = new File(indexDir, index.getName() + TYPE_FILE_EXT);
        IndexLoader loader = index.getIndexLoader();
        writeNameToFile(typefile, loader);
        index.save(indexDir);
    }

    @Override
    public void remount(File newdir, boolean datafilesonly) throws AxionException {
        closeFiles();
        initFiles(newdir, datafilesonly);
        writeMetaFile();
        resetLobColumns();
        super.remount(newdir, datafilesonly);
    }

    @Override
    public void removeIndex(Index index) throws AxionException {
        super.removeIndex(index);
        File indexdir = new File(_indexRootDir, index.getName());
        if (!FileUtil.delete(indexdir)) {
            throw new AxionException("Unable to delete \"" + indexdir + "\" during remove index " + index.getName());
        }
    }

    public void rename(String oldName, String newName, Properties newTableProp) throws AxionException {
        try {
            closeFiles();
            if (newTableProp == null) {
                renameTableFiles(oldName, newName);
            } else {
                renameTableFiles(oldName, newName, newTableProp);
            }
            super.rename(oldName, newName);

            clearDataFileReference();
            writeMetaFile(); // refresh meta file
            initFiles(getRootDir(), false);

            resetLobColumns();
        } catch (Exception e) {
            throw new AxionException("Fail to alter table: " + newName);
        }
    }

    @Override
    public void rename(String oldName, String newName) throws AxionException {
        rename(oldName, newName, null);
    }

    @Override
    public void setSequence(Sequence seq) throws AxionException {
        super.setSequence(seq);
        checkpoint();
    }

    @Override
    public void shutdown() throws AxionException {
        closeFiles();
    }

    public void truncate() throws AxionException {
        // Return immediately if table is already empty.
        if (getRowCount() == 0) {
            return;
        }

        // Rename old data file as backup and replace with zero-length file.
        File df = getDataFile();
        File pidxFile = getTableFile(PIDX_FILE_EXT);
        File bkupFile = new File(df.getParentFile(), df.getName() + ".backup");
        File bkupPidxFile = new File(pidxFile + ".backup");
        FileUtil.assertFileNotLocked(bkupFile);

        try {
            closeFiles();
            FileUtil.delete(bkupFile);
            if(df.renameTo(bkupFile)!=true){
                FileUtil.truncate(df, 0);
            }
            if(pidxFile.renameTo(bkupPidxFile)!=true){
                FileUtil.truncate(pidxFile, 0);
            }

            reloadFilesAfterTruncate();
            truncateIndices();
            saveIndicesAfterTruncate();

            FileUtil.delete(bkupFile);
            FileUtil.delete(bkupPidxFile);
        } catch (Exception e) {
            // Restore old file and reload it.
            closeFiles();
            FileUtil.delete(df);
            FileUtil.delete(pidxFile);
            bkupFile.renameTo(df);
            bkupPidxFile.renameTo(pidxFile);
            reloadFilesAfterTruncate();
            recreateIndices();
            throw new AxionException(e);
        }
    }
    //--------------------------------------------------------------- Protected

    protected void clearDataFileReference() {
        _dataFile = null;
    }

    protected void closeFiles() {
        if (null != _readStream) {
            try {
                _readStream.close();
            } catch (IOException e) {
                // ignored
            }
            _readStream = null;
        }

        if (null != _writeStream) {
            try {
                _writeStream.close();
            } catch (IOException e) {
                // ignored
            }
            _writeStream = null;
        }

        if (null != _pidx) {
            try {
                _pidx.close();
            } catch (IOException e) {
                // ignored
            }
            _pidx = null;
        }
    }

    protected void createOrLoadDataFile() throws AxionException {
        if (!isReadOnly()) {
            FS.createNewFile(getDataFile());
        }
        getOutputStream();
        getInputStream();
    }

    protected void createOrLoadFreeIdsFile() throws AxionException {
        if (_freeIds == null) {
            File freeIdsFile = getTableFile(FRID_FILE_EXT);
            if (!isReadOnly()) {
                FS.createNewFile(freeIdsFile);
            }
            _freeIds = FS.parseIntFile(freeIdsFile);
        }
    }

    protected void loadOrMigrateMetaFile(Database db) throws AxionException {
        migrate(db);
    }

    protected abstract File getDataFile();

    protected String getDefaultDataFileExtension() {
        return "DATA";
    }

    protected synchronized BufferedDataInputStream getInputStream() throws AxionException {
        if (null == _readStream) {
            _readStream = FS.openBufferedDIS(getDataFile());
        }
        return _readStream;
    }

    protected abstract File getLobDir();

    protected synchronized BufferedDataOutputStream getOutputStream() throws AxionException {
        if (!isReadOnly() && null == _writeStream) {
            _writeStream = FS.openBufferedDOSAppend(getDataFile(), 1024);
        }
        return _writeStream;
    }

    protected synchronized AxionFileSystem.PidxList getPidxList() {
        if (_pidx == null) {
            File pidxFile = getTableFile(PIDX_FILE_EXT);
            try {
                if (!isReadOnly()) {
                    FS.createNewFile(pidxFile);
                }
                _pidx = parsePidxFile(pidxFile);
            } catch (AxionException e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
            _nextFreeId = -1;
        }
        return _pidx;
    }

    protected File getRootDir() {
        return _dir;
    }

    protected RowIterator getRowIterator() throws AxionException {
        return new BaseRowIterator() {

            Row _current = null;
            int _currentId = -1;
            int _currentIndex = -1;
            int _nextId = 0;
            int _nextIndex = 0;

            public Row current() {
                if (!hasCurrent()) {
                    throw new NoSuchElementException("No current row.");
                }
                return _current;
            }

            public int currentIndex() {
                return _currentIndex;
            }

            public boolean hasCurrent() {
                return null != _current;
            }

            public boolean hasNext() {
                return nextIndex() < getRowCount();
            }

            public boolean hasPrevious() {
                return nextIndex() > 0;
            }

            @Override
            public Row last() throws AxionException {
                if (isEmpty()) {
                    throw new IllegalStateException("No rows in table.");
                }
                _nextIndex = getRowCount();
                _nextId = getPidxList().size();
                previous();

                _nextId++;
                _nextIndex++;
                return current();
            }

            public Row next() throws AxionException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next row");
                }
                next(1);
                setCurrentRow();
                return current();
            }

            @Override
            public int next(int count) throws AxionException {
                for (int start = 0; start < count;) {
                    _currentId = _nextId++;
                    if (!hasNext() || _currentId > getPidxList().size()) {
                        throw new NoSuchElementException("No next row");
                    } else if (getPidxList().get(_currentId) != INVALID_OFFSET) {
                        _currentIndex = _nextIndex++;
                        start++;
                    }
                }
                _current = null;
                return _currentId;
            }

            public int nextIndex() {
                return _nextIndex;
            }

            public Row previous() throws AxionException {
                if (!hasPrevious()) {
                    throw new NoSuchElementException("No previous row");
                }
                previous(1);
                setCurrentRow();
                return current();
            }

            @Override
            public int previous(int count) throws AxionException {
                for (int start = 0; start < count;) {
                    _currentId = --_nextId;
                    if (!hasPrevious() || _currentId < 0) {
                        throw new NoSuchElementException("No Previous row");
                    } else if (getPidxList().get(_currentId) != INVALID_OFFSET) {
                        _currentIndex = --_nextIndex;
                        start++;
                    }
                }
                _current = null;
                return _currentId;
            }

            public int previousIndex() {
                return _nextIndex - 1;
            }

            @Override
            public void remove() throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                deleteRow(_current);
                _nextIndex--;
                _currentIndex = -1;
            }

            public void reset() {
                _current = null;
                _nextIndex = 0;
                _currentIndex = -1;
                _nextId = 0;
            }

            @Override
            public void set(Row row) throws AxionException {
                if (-1 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                updateRow(_current, row);
            }

            @Override
            public int size() throws AxionException {
                return getRowCount();
            }

            @Override
            public String toString() {
                return "DiskTable(" + getName() + ")";
            }

            private Row setCurrentRow() throws AxionException {
                Row row = getRowByOffset(_currentId, getPidxList().get(_currentId));
                if (row != null) {
                    return _current = row;
                }
                throw new IllegalStateException("No valid row at position " + _currentIndex);
            }
        };
    }

    protected abstract Row getRowByOffset(int idToAssign, long ptr) throws AxionException;

    protected File getTableFile(String extension) {
        return new File(getRootDir(), getName().toUpperCase() + extension);
    }

    protected boolean isReadOnly() {
        return _readOnly;
    }

    protected void initFiles(File basedir, boolean datafilesonly) throws AxionException {
        if (!datafilesonly) {
            _dir = basedir;
            _indexRootDir = new File(_dir, INDICES_DIR_NAME);
        }
        clearDataFileReference();
        getDataFile();
        _readStream = getInputStream();
        _writeStream = getOutputStream();
    }

    protected void initializeRowCount() throws AxionException {
        _rowCount = 0;
        for (int i = 0, I = getPidxList().size(); i < I; i++) {
            long ptr = getPidxList().get(i);
            if (ptr != INVALID_OFFSET) {
                _rowCount++;
            }
        }
    }

    protected synchronized AxionFileSystem.PidxList parsePidxFile(File pidxFile) throws AxionException {
        return FS.parseLongPidxList(pidxFile, _readOnly);
    }

    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        // Allow sub class to read extra meta info
    }

    protected abstract void reloadFilesAfterTruncate() throws AxionException;

    protected void renameTableFiles(String oldName, String name) {
        // rename table dir
        File olddir = new File(_dbdir, oldName);
        _dir = new File(_dbdir, name);
        olddir.renameTo(_dir);

        // rename table files
        FileUtil.renameFile(_dir, oldName, name, TYPE_FILE_EXT);
        FileUtil.renameFile(_dir, oldName, name, PIDX_FILE_EXT);
        FileUtil.renameFile(_dir, oldName, name, FRID_FILE_EXT);
        FileUtil.renameFile(_dir, oldName, name, META_FILE_EXT);
        FileUtil.renameFile(_dir, oldName, name, SEQ_FILE_EXT);
    }

    protected void renameTableFiles(String oldName, String name, Properties newTblProps) {
        renameTableFiles(oldName, name, null);
    }

    protected void saveIndicesAfterTruncate() throws AxionException {
        for (Iterator iter = getIndices(); iter.hasNext();) {
            Index index = (Index) (iter.next());
            saveIndexAfterTruncate(index);
        }
    }

    protected void tryToRemove(RowIterator iter) throws AxionException {
        try {
            iter.remove();
        } catch (UnsupportedOperationException e) {
            // ignore it, that's OK
        }
    }

    protected final void writeFridFile() throws AxionException {
        FS.writeIntFile(getTableFile(FRID_FILE_EXT), _freeIds);
    }

    protected void writeMetaFile() throws AxionException {
        ObjectOutputStream out = null;
        File metaFile = getTableFile(META_FILE_EXT);
        try {
            out = FS.createObjectOutputSteam(metaFile);
            out.writeInt(CURRENT_META_VERSION);
            writeColumns(out);
            out.flush();
            writeConstraints(out);
            out.flush();
            writeTableProperties(out);
            out.flush();
        } catch (IOException e) {
            throw new AxionException("Unable to write meta file " + metaFile + " for table " + getName(), e);
        } finally {
            FS.closeOutputStream(out);
        }
    }

    protected void writeNameToFile(File file, Object obj) throws AxionException {
        ObjectOutputStream out = null;
        try {
            out = FS.createObjectOutputSteam(file);
            out.writeUTF(obj.getClass().getName());
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            FS.closeOutputStream(out);
        }
    }

    protected abstract void writeRow(BufferedDataOutputStream buffer, Row row) throws AxionException;

    protected void writeTableProperties(ObjectOutputStream out) throws AxionException {
        // Allow the subclass to write table specific metadata
    }

    private void applyDeletesToRows(IntCollection rowids) throws AxionException {
        try {
            IntIterator iter = rowids.iterator();
            if (iter.hasNext()) {
                for (int rowid; iter.hasNext();) {
                    rowid = iter.next();
                    if (rowid > getPidxList().size() - 1) {
                        throw new AxionException("Can't delete non-existent row");
                    }
                    getPidxList().set(rowid, INVALID_OFFSET);
                    //_freeIds.add(rowid);
                    _rowCount--;
                }
                _pidx.flush();
                _freeIds.addAll(rowids);
                writeFridFile();
            }
        } catch (IOException e) {
            throw new AxionException("Error writing data.", e);
        }
    }

    private void applyInsertsToRows(RowIterator rows) throws AxionException {
        try {
            // write all the rows to a buffer
            BufferedDataOutputStream out = getOutputStream();
            while (rows.hasNext()) {
                Row row = rows.next();
                _rowCount++;
                if (!_freeIds.isEmpty() && _freeIdPos > -1) {
                    getPidxList().set(_freeIds.removeElementAt(0), out.getPos());
                    _freeIdPos--;
                } else {
                    getPidxList().add(out.getPos());
                }
                writeRow(out, row);

                // drop the reference to the row form the iterator
                tryToRemove(rows);
            }

            _writeStream.flush();
            _pidx.flush();
            writeFridFile();
            _nextFreeId = -1;
        } catch (IOException e) {
            throw new AxionException("Error writing data.", e);
        }
    }

    private void applyUpdatesToRows(RowIterator rows) throws AxionException {
        try {
            BufferedDataOutputStream out = getOutputStream();
            while (rows.hasNext()) {
                Row row = rows.next();
                if (row.getIdentifier() > getPidxList().size() - 1) {
                    throw new AxionException("Can't update non-existent row");
                }
                // update the slot in the pidx file to point to the new data
                getPidxList().set(row.getIdentifier(), out.getPos());
                writeRow(out, row);

                // drop the reference to the row form the iterator
                tryToRemove(rows);
            }

            _writeStream.flush();
            _pidx.flush();
            getInputStream().reset();
        } catch (IOException e) {
            throw new AxionException("Error writing data.", e);
        }
    }
    // ----------------------------------------------------------------- Private

    private void createOrLoadTableFiles(String name, Database db, TableFactory factory) throws AxionException {
        synchronized (BaseDiskTable.class) {
            _dir = new File(db.getDBDirectory(), name.toUpperCase());

            if (!_dir.exists()) {
                if (!_dir.mkdirs()) {
                    throw new AxionException("Unable to create directory \"" + _dir + "\" for Table \"" + name + "\".");
                }
            }

            // create the type file if it doesn't already exist
            File typefile = getTableFile(TYPE_FILE_EXT);
            if (!typefile.exists()) {
                writeNameToFile(typefile, factory);
            }

            _freeIds = new ArrayIntList();

            loadOrMigrateMetaFile(db); // note order is signficant here!
            createOrLoadFreeIdsFile();

            initializeRowCount();

            // indices - directory containing index files
            _indexRootDir = new File(_dir, INDICES_DIR_NAME);
            if (_indexRootDir.exists()) {
                loadIndices(_indexRootDir, db);
            } else {
                _indexRootDir.mkdirs();
            }

            File seqfile = getTableFile(SEQ_FILE_EXT);
            if (seqfile.exists()) {
                loadSequences();
            }
        }
    }

    private void loadIndices(File parentdir, Database db) throws AxionException {
        String[] indices = parentdir.list(DOT_TYPE_FILE_FILTER);
        for (int i = 0; i < indices.length; i++) {
            File indexdir = new File(parentdir, indices[i]);
            File typefile = new File(indexdir, indices[i] + TYPE_FILE_EXT);

            String loadername = null;
            ObjectInputStream in = null;
            try {
                in = FS.openObjectInputSteam(typefile);
                loadername = in.readUTF();
            } catch (IOException e) {
                throw new AxionException(e);
            } finally {
                FS.closeInputStream(in);
            }

            IndexLoader loader = null;
            try {
                Class clazz = Class.forName(loadername);
                loader = (IndexLoader) (clazz.newInstance());
            } catch (Exception e) {
                throw new AxionException(e);
            }
            Index index = loader.loadIndex(this, indexdir);
            db.addIndex(index, this);
        }
    }

    private void loadSequences() throws AxionException {
        File seqFile = getTableFile(SEQ_FILE_EXT);
        DataInputStream in = null;
        if (seqFile.exists()) {
            try {
                in = FS.openDataInputSteam(seqFile);
                Sequence seq = new Sequence();
                seq.read(in);
                super.setSequence(seq);
            } catch (Exception e) {
                throw new AxionException("Unable to read sequence file", e);
            } finally {
                FS.closeInputStream(in);
            }
        }
    }

    private void parseV0MetaFile(ObjectInputStream in) throws IOException, AxionException {
        int I = in.readInt(); // read number of columns
        for (int i = 0; i < I; i++) {
            String name = in.readUTF(); // read column name
            String dtypename = in.readUTF(); // read data type class name
            // create instance of datatype
            DataType type = null;
            try {
                Class clazz = Class.forName(dtypename);
                type = (DataType) (clazz.newInstance());
            } catch (Exception e) {
                throw new AxionException("Can't load table " + getName() + ", data type " + dtypename + " not found.", e);
            }
            addColumn(new Column(name, type), false);
        }
    }

    private void parseV1MetaFile(ObjectInputStream in, Database db) throws AxionException, IOException, ClassNotFoundException {
        readColumns(in);
        readConstraints(in, db);
    }

    // FIXME: there ought to be a better way to do this
    private void resetLobColumn(Column col) throws AxionException {
        if (col.getDataType() instanceof LOBType) {
            LOBType lob = (LOBType) (col.getDataType());
            lob.setLobDir(new File(getLobDir(), col.getName().toUpperCase()));
        }
    }

    protected void resetLobColumns() throws AxionException {
        for (int i = 0, I = getColumnCount(); i < I; i++) {
            Column col = getColumn(i);
            resetLobColumn(col);
        }
    }

    private void saveIndex(Index index) throws AxionException {
        File dataDir = new File(_indexRootDir, index.getName());
        index.save(dataDir);
    }

    private void saveIndexAfterTruncate(Index index) throws AxionException {
        File dataDir = new File(_indexRootDir, index.getName());
        index.saveAfterTruncate(dataDir);
    }
    private static final FilenameFilter DOT_TYPE_FILE_FILTER = new FilenameFilter() {

        public boolean accept(File dir, String name) {
            File file = new File(dir, name);
            if (file.isDirectory()) {
                File idx = new File(file, name + TYPE_FILE_EXT);
                if (idx.exists()) {
                    return true;
                }
            }
            return false;
        }
    };
    //--------------------------------------------------------------- Attributes
    protected static AxionFileSystem FS = new AxionFileSystem();
    protected static final long INVALID_OFFSET = Long.MAX_VALUE;//ArrayUnsignedIntList.MAX_VALUE;
    protected static final int CURRENT_META_VERSION = 4;
    protected static final String FRID_FILE_EXT = ".FRID";
    protected static final String INDICES_DIR_NAME = "INDICES";
    protected static final String META_FILE_EXT = ".META";
    protected static final String PIDX_FILE_EXT = ".PIDX";
    protected static final String SEQ_FILE_EXT = ".SEQ";
    protected static final String TYPE_FILE_EXT = ".TYPE";
    /** The name of my ".data" file. */
    protected File _dataFile = null;
    protected File _dbdir = null;
    /** List of free ids. */
    protected IntList _freeIds = null;
    private int _nextFreeId = -1;
    private int _freeIdPos = -1;
    /** List of offsets into the .data file, by row id. */
    private AxionFileSystem.PidxList _pidx = null;
    protected boolean _readOnly = false;
    protected int _rowCount = 0;
    /** The directory in which my data are stored. */
    private File _dir = null;
    /** The directory in which my indices are stored. */
    private File _indexRootDir = null;
    protected BufferedDataInputStream _readStream = null;
    protected BufferedDataOutputStream _writeStream = null;
    private static final Logger _log = Logger.getLogger(BaseDiskTable.class.getName());
}

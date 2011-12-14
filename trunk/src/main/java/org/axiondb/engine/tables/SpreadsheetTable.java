/*
 * SpreadsheetTable.java
 *
 * Created on March 13, 2007, 6:08 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.axiondb.engine.tables;

import java.io.EOFException;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import jxl.CellType;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;
import jxl.write.Label;
import jxl.write.WritableCell;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.Number;
import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.Constraint;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Index;
import org.axiondb.IndexLoader;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.TableFactory;
import org.axiondb.TableOrganizationContext;
import org.axiondb.engine.rowiterators.BaseRowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.FileUtil;
import org.axiondb.types.CharacterType;
import org.axiondb.types.StringType;
import org.axiondb.util.ExceptionConverter;

/**
 *
 * @author karthikeyan s
 * @author jawed
 */
public class SpreadsheetTable extends BaseTable implements ExternalTable {

    /** The name of my ".data" file. */
    protected File _dataFile = null;
    protected File _dbdir = null;
    protected boolean _readOnly = false;
    private int _rowCount = -1;
    private int _currentRow = -1;
    protected int _rowsToSkip = 0;
    private File _dir;
    protected String _fileName;
    protected String _sheetName;
    private File _indexRootDir = null;
    protected boolean _isFirstLineHeader = false;
    private boolean _isCreateDataFileIfNotExist = true;
    private boolean _validate = true;
    private Properties prop = new Properties();

    public static final String PROP_FILENAME = "FILENAME";
    public static final String PROP_SHEET = "SHEET";
    public static final String PROP_ISFIRSTLINEHEADER = "ISFIRSTLINEHEADER";
    public static final String PROP_ROWSTOSKIP = "ROWSTOSKIP";
    public static final String PROP_VALIDATION = "VALIDATION";
    public static final String PROP_MAXFAULTS = "MAXFAULTS";

    protected static final String PIDX_FILE_EXT = ".PIDX";
    protected static final String META_FILE_EXT = ".META";
    protected static final String TYPE_FILE_EXT = ".TYPE";
    protected static final String INDICES_DIR_NAME = "INDICES";

    public static String PARAM_KEY_FILE_NAME = "SPREADSHEET_FILE_NAME";
    public static String PARAM_KEY_TEMP_FILE_NAME = "TEMP_SPREADSHEET_FILE_NAME";
    public static String PARAM_KEY_FILE_DIR = "SPREADSHEET_FILE_DIR";

    private AxionFileSystem.PidxList _pidx = null;
    protected static final long INVALID_OFFSET = Long.MAX_VALUE;
    protected int _maxFaults = Integer.MAX_VALUE;
    private int rowPos = 0;

    protected TableOrganizationContext context;
    private Sheet sheet;
    private Workbook workbook;
    private WritableWorkbook writeableWorkbook;
    protected static AxionFileSystem FS = new AxionFileSystem();
    protected static final int CURRENT_META_VERSION = 3;

    private static final Set PROPERTY_KEYS = new HashSet(3);
    private static final Set REQUIRED_KEYS = new HashSet(1);
    static {
        PROPERTY_KEYS.add(PROP_FILENAME);
        PROPERTY_KEYS.add(PROP_SHEET);
        PROPERTY_KEYS.add(PROP_VALIDATION);
        PROPERTY_KEYS.add(PROP_CREATE_IF_NOT_EXIST);
        PROPERTY_KEYS.add(PROP_ISFIRSTLINEHEADER);
        PROPERTY_KEYS.add(PROP_ROWSTOSKIP);
        PROPERTY_KEYS.add(PROP_MAXFAULTS);
        PROPERTY_KEYS.add(ExternalTable.PROP_LOADTYPE);
    }

    /** Creates a new instance of XMLTable */
    public SpreadsheetTable(String name, Database db) throws AxionException {
        super(name);
        setType(ExternalTable.SPREADSHEET_TABLE_TYPE);
        _dbdir = db.getDBDirectory();
        _readOnly = db.isReadOnly();
        createOrLoadTableFiles(name, db, new SpreadsheetTableLoader());
    }

    public void applyDeletes(IntCollection rowids) throws AxionException {
        synchronized (this) {
            try {
                int rowid;
                for (IntIterator iter = rowids.iterator(); iter.hasNext();) {
                    rowid = iter.next();
                    getPidxList().set(rowid, INVALID_OFFSET);
                    _rowCount--;
                }
                _pidx.flush();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }

    public void addConstraint(Constraint constraint) throws AxionException {
        super.addConstraint(constraint);
        writeMetaFile();
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

    public void applyInsertsToRows(RowIterator rows) throws AxionException {
        synchronized (this) {
            try {
                int colCount = sheet.getColumns();
                if (colCount == 0) {
                    colCount = getColumnCount();
                }
                writeableWorkbook = getWriteableWorkbook();
                while (rows.hasNext()) {
                    Row row = rows.next();
                    RowEvent event = new RowInsertedEvent(this, null, row);
                    WritableSheet writeableSheet = writeableWorkbook.getSheet(_sheetName);
                    writeableSheet.insertRow(rowPos);
                    for (int i = 0; i < colCount; i++) {
                        String colValue = String.valueOf(row.get(i));
                        if(colValue.equalsIgnoreCase("null")){
                            colValue = "";
                        }
                        Label label = new Label(i, rowPos, colValue);
                        writeableSheet.addCell(label);
                    }
                    getPidxList().add(rowPos);
                    rowPos++;
                    _rowCount++;
                }
                _pidx.flush();
                writeableWorkbook.write();
                writeableWorkbook.close();
                refreshSheet();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }

    public void applyUpdates(RowCollection rows) throws AxionException {
        synchronized (this) {
            try {
                int colCount = sheet.getColumns();
                writeableWorkbook = getWriteableWorkbook();
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row newrow = iter.next();
                    Row oldrow = getRow(newrow.getIdentifier());
                    if (oldrow != null) {
                        RowEvent event = new RowUpdatedEvent(this, oldrow, newrow);
                    }
                    WritableSheet writeableSheet = writeableWorkbook.getSheet(_sheetName);
                    for (int i = 0; i < colCount; i++) {
                        Label label = new Label(i, newrow.getIdentifier() - 1, String.valueOf(newrow.get(i)));
                        WritableCell cell = writeableSheet.getWritableCell(i, newrow.getIdentifier() - 1);
                        cell.setCellFormat(cell.getCellFormat());
                        if (cell.getType() == CellType.LABEL) {
                            Label l = (Label) cell;
                            l.setString(String.valueOf(newrow.get(i)));
                        } else if (cell.getType() == CellType.NUMBER) {
                            Number number = (Number) cell;
                            number.setValue(Double.valueOf(String.valueOf(newrow.get(i))));
                        }
                    }
                }
                writeableWorkbook.write();
                writeableWorkbook.close();
                refreshSheet();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }

    public void freeRowId(int id) {
    }

    public int getNextRowId() {
        return _currentRow + 1;
    }

    public int getRowCount() {
        return _rowCount;
    }

    public void populateIndex(Index index) throws AxionException {
        Row row = null;
        for (int i = 0, I = getPidxList().size(); i < I; i++) {
            long ptr = getPidxList().get(i);
            if (ptr != INVALID_OFFSET) {
                row = getRow((int) ptr);
                index.rowInserted(new RowInsertedEvent(this, null, row));
                row.setIdentifier(i);
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

    public void removeIndex(Index index) throws AxionException {
        super.removeIndex(index);
        File indexdir = new File(_indexRootDir, index.getName());
        if (!FileUtil.delete(indexdir)) {
            throw new AxionException("Unable to delete \"" + indexdir + "\" during remove index " + index.getName());
        }
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

    protected void writeHeader(WritableSheet sheet) {
        try {
            if (_isFirstLineHeader) {
                for (int i = 0; i < getColumnCount(); i++) {
                    String colValue = getColumn(i).getName();
                    Label label = new Label(i, 0, colValue);
                    sheet.addCell(label);
                }
            }
        } catch (Exception ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
    }

    protected void createOrLoadDataFile() throws AxionException {
        createOrLoadDataFile(_isCreateDataFileIfNotExist);
    }

    protected void createOrLoadDataFile(boolean createNewDataFile) throws AxionException {
        File file = getDataFile();
        try {
            if (createNewDataFile && !file.exists()) {
                WritableWorkbook writeableWB = Workbook.createWorkbook(file, getWorkbookSettings());
                WritableSheet wSheet = writeableWB.createSheet(_sheetName, 0);
                if (_isFirstLineHeader) {
                    writeHeader(wSheet);
                }
                writeableWB.write();
                writeableWB.close();
            } else {
                workbook = Workbook.getWorkbook(file, getWorkbookSettings());
                sheet = workbook.getSheet(_sheetName);
                rowPos = sheet.getRows();
            }
        } catch (Exception ioex) {
            throw new AxionException("Unable to create data file \"" + getDataFile() + "\" for table " + getName() + ".", ioex);
        }
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
        }
        return _pidx;
    }

    protected boolean isReadOnly() {
        return _readOnly;
    }

    protected synchronized AxionFileSystem.PidxList parsePidxFile(File pidxFile) throws AxionException {
        return FS.parseLongPidxList(pidxFile, _readOnly);
    }

    private void saveIndex(Index index) throws AxionException {
        File dataDir = new File(_indexRootDir, index.getName());
        index.save(dataDir);
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

    protected int ignoreRowsToSkip() {
        if (_rowsToSkip > 0) {
            return _rowsToSkip;
        } else if (_isFirstLineHeader) {
            // Handle old versions of flatfiles where RowsToSkip is undefined and first
            // line header is true.
            return 1;
        }
        return 0;
    }

    public Row getRow(int id) throws AxionException {
        Row row;
        try {
            int colCount = sheet.getColumns();
            row = new SimpleRow(id, colCount);
            for (int i = 0; i < colCount; i++) {
                String columnValue = sheet.getCell(i, id).getContents();
                row = trySettingColumn(row, i, columnValue);
            }
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
        _currentRow = id;
        return row;
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
                return "SpreadsheetTable(" + getName() + ")";
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

    public void truncate() throws AxionException {
        File pidxFile = getTableFile(PIDX_FILE_EXT);
        File bkupPidxFile = new File(pidxFile + ".backup");

        try {
            closeFiles();
            if (pidxFile.renameTo(bkupPidxFile) != true) {
                FileUtil.truncate(pidxFile, 0);
            }

            reloadFilesAfterTruncate();
            truncateIndices();
            saveIndicesAfterTruncate();

            FileUtil.delete(bkupPidxFile);
        } catch (Exception ex) {
            // Restore old file and reload it.
            closeFiles();
            FileUtil.delete(pidxFile);
            bkupPidxFile.renameTo(pidxFile);
            reloadFilesAfterTruncate();
            recreateIndices();
            throw new AxionException(ex);
        }
    }

    protected void reloadFilesAfterTruncate() throws AxionException {
        try {

            // Create zero-record data file (with header if required).
            jxl.Workbook wBook = jxl.Workbook.getWorkbook(getDataFile());
            int index = -1;
            for (int i = 0; i < wBook.getNumberOfSheets(); i++) {
                if (wBook.getSheet(i).getName().equals(_sheetName)) {
                    index = i;
                    break;
                }
            }
            wBook.close();

            // implement remove sheet and insert sheet at index.
            WritableWorkbook writeableWorkbook = getWriteableWorkbook();
            writeableWorkbook.removeSheet(index);
            writeableWorkbook.createSheet(_sheetName, index);
            writeableWorkbook.write();
            writeableWorkbook.close();
            workbook.close();
            workbook = null;
            sheet = null;

            initFiles(getDataFile(), true);

            initializeTable();
            initializeRowCount();
        } catch (Exception ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
    }

    protected void saveIndicesAfterTruncate() throws AxionException {
        for (Iterator iter = getIndices(); iter.hasNext();) {
            Index index = (Index) (iter.next());
            saveIndexAfterTruncate(index);
        }
    }

    private void saveIndexAfterTruncate(Index index) throws AxionException {
        File dataDir = new File(_indexRootDir, index.getName());
        index.saveAfterTruncate(dataDir);
    }

    public boolean loadExternalTable(Properties prop) throws AxionException {
        try {
            if (context == null) {
                context = new SpreadsheetTableOrganizationContext();
            }
            context.readOrSetDefaultProperties(prop);
            context.updateProperties();

            createOrLoadDataFile();
            initializeTable();

            writeMetaFile();
            return true;
        } catch (Exception e) {
            // If we fail to initialize the flatfile table then drop the table.
            // Table creation is not atomic in case of flat file.
            try {
                drop();
            } catch (Throwable ignore) {
                // this exception is secondary to the one we want to throw...
            }
            throw new AxionException("Failed to create table using supplied properties. ", e);
        }
    }

    @Override
    public void drop() throws AxionException {
        super.drop();
        closeFiles();
        if (!FileUtil.delete(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table " + getName());
        }
    }

    public void shutdown() throws AxionException {
        closeFiles();
    }

    protected void closeFiles() {
        if (null != sheet) {
            sheet = null;
        }
        if (null != writeableWorkbook) {
            try {
                writeableWorkbook.close();
            } catch (Exception ignore) {
                //ignore
            }
            writeableWorkbook = null;
        }
        if (null != workbook) {
            try {
                workbook.close();
            } catch (Exception e) {
                // ignored
            }
            workbook = null;
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

    protected void clearDataFileReference() {
        _dataFile = null;
    }

    public void remount() throws AxionException {
        try {
            Workbook workbook = Workbook.getWorkbook(getDataFile(), getWorkbookSettings());
            sheet = workbook.getSheet(_sheetName);
        } catch (Exception ex) {
            Logger.getLogger("global").log(Level.SEVERE, null, ex);
        }
    }

    protected void initFiles(File basedir, boolean datafilesonly) throws AxionException {
        if (!datafilesonly) {
            _dir = basedir;
            _indexRootDir = new File(_dir, INDICES_DIR_NAME);
        }
        clearDataFileReference();
        getDataFile();
    }

    protected boolean isNullString(String str) {
        return str == null || str.trim().length() == 0;
    }

    private String getDefaultDataFileExtension() {
        return "xls";
    }

    private void createOrLoadTableFiles(String name, Database db, TableFactory factory) throws AxionException {
        synchronized (SpreadsheetTable.class) {
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
            loadOrMigrateMetaFile(db);
            initializeRowCount();

            // indices - directory containing index files
            _indexRootDir = new File(_dir, INDICES_DIR_NAME);
            if (_indexRootDir.exists()) {
                loadIndices(_indexRootDir, db);
            } else {
                _indexRootDir.mkdirs();
            }
        }
    }

    protected void loadOrMigrateMetaFile(Database db) throws AxionException {
        migrate(db);
    }

    /** Migrate from older version to newer version for this table */
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

    public Properties getTableProperties() {
        return context.getTableProperties();
    }

    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        try {
            _isFirstLineHeader = Boolean.valueOf(in.readUTF()).booleanValue();
            _isCreateDataFileIfNotExist = Boolean.valueOf(in.readUTF()).booleanValue();
            _fileName = in.readUTF();
            _sheetName = in.readUTF();
            _rowsToSkip = in.readInt();
            _maxFaults = in.readInt();
            _validate = Boolean.valueOf(in.readUTF()).booleanValue();
            
            context = new SpreadsheetTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            createOrLoadDataFile();
        } catch (IOException ioex) {
            throw new AxionException("Unable to parse meta file for table " + getName(), ioex);
        }
    }

    public void addColumn(Column col, boolean metaUpdateNeeded) throws AxionException {
        super.addColumn(col);
        if (metaUpdateNeeded) {
            writeMetaFile();
        }
    }

    private void parseV1MetaFile(ObjectInputStream in, Database db) throws AxionException, IOException, ClassNotFoundException {
        readColumns(in);
        readConstraints(in, db);
    }

    private void writeMetaFile() throws AxionException {
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

    protected void writeTableProperties(ObjectOutputStream out) throws AxionException {
        try {
            if (_fileName != null) {
                out.writeUTF(Boolean.toString(_isFirstLineHeader));
                out.writeUTF(Boolean.toString(_isCreateDataFileIfNotExist));
                out.writeUTF(_fileName);
                out.writeUTF(_sheetName);
                out.writeInt(_rowsToSkip);
                out.writeInt(_maxFaults);
                out.writeUTF(Boolean.toString(_validate));
                out.flush();
            }
        } catch (IOException ioex) {
            throw new AxionException("Unable to write meta file for table " + getName(), ioex);
        } finally {
            FS.closeOutputStream(out);
        }
    }

    protected void writeNameToFile(File file, Object obj) throws AxionException {
        ObjectOutputStream out = null;
        try {
            out = FS.createObjectOutputSteam(file);
            out.writeUTF(obj.getClass().getName());
            out.flush();
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            FS.closeOutputStream(out);
        }
    }

    protected Row getRowByOffset(int idToAssign, long ptr) throws AxionException {
        Row row;
        synchronized (this) {
            try {
                if(null == sheet){
                    refreshSheet();
                }
                int colCount = sheet.getColumns();
                row = new SimpleRow(idToAssign, colCount);
                for (int i = 0; i < colCount; i++) {
                    String columnValue = sheet.getCell(i, (int) ptr).getContents();
                    row = trySettingColumn(row, i, columnValue);
                }
            } catch (Exception e) {
                if (e instanceof AxionException) {
                    throw (AxionException) e;
                }
                throw new AxionException(e);
            }
        }
        return row;
    }

    private Row trySettingColumn(Row row, int i, String colValue) throws AxionException {
        DataType columnDataType = getColumn(i).getDataType();

        colValue = evaluateForNull(colValue, columnDataType);
        if (colValue.length() == 0) {
            row.set(i, null);
        } else {
            Object val = columnDataType.convert(colValue);
            row.set(i, val);
        }

        return row;
    }

    private String evaluateForNull(String colValue, DataType datatype) {
        if (null == colValue) {
            return null;
        } else if (datatype instanceof CharacterType) {
            int colWidth = datatype.getPrecision();
            return (colWidth <= 0 || (colValue.length() == colWidth && colValue.trim().length() == 0)) ? null : colValue;
        } else if (!(datatype instanceof StringType) && colValue.trim().length() == 0) {
            return null;
        }

        return colValue;
    }

    private WritableWorkbook getWriteableWorkbook() throws IOException, BiffException {
        if (workbook == null) {
            refreshSheet();
        }
        writeableWorkbook = Workbook.createWorkbook(getDataFile(), workbook, getWorkbookSettings());
        return writeableWorkbook;
    }

    protected void initializeTable() throws AxionException {
        try {
            if (workbook == null) {
                workbook = Workbook.getWorkbook(getDataFile(), getWorkbookSettings());
            }
            int faultCount = 0;
            _rowCount = 0;
            rowPos = ignoreRowsToSkip();
            sheet = workbook.getSheet(_sheetName);
            int nofRows = sheet.getRows();
            int pos = ignoreRowsToSkip();
            AxionFileSystem.PidxList pidx = getPidxList();
            while (pos < nofRows) {
                if (nofRows == pidx.size() + ignoreRowsToSkip()) {
                    break;
                }
                pidx.add(pos);
                if (_validate) {
                    try {
                        Row row = new SimpleRow(rowPos, getColumnCount());
                        for (int i = 0; i < getColumnCount(); i++) {
                            String columnValue = sheet.getCell(i, rowPos).getContents();
                            row = trySettingColumn(row, i, columnValue);
                        }
                        _rowCount++;
                    } catch (AxionException ex) {
                        // set current position to invalid
                        pidx.set(pidx.size() - 1, INVALID_OFFSET);
                        if (++faultCount > _maxFaults) {
                            // TODO: Write bad rows to a file with .bad extension
                            String msg = "Fault tolerance threshold (" + _maxFaults + ") exceeded for table " + getName() + ". ";
                            throw new AxionException(msg + ex.getMessage(), ex);
                        }
                    }
                    rowPos++;
                } else {
                    _rowCount++;
                }
                pos++;
            }
            _currentRow = 0;
            pidx.flush();
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }

    private void refreshSheet() throws IOException, BiffException {
        if (workbook != null) {
            workbook.close();
        }
        workbook = Workbook.getWorkbook(getDataFile(), getWorkbookSettings());
        sheet = workbook.getSheet(_sheetName);
    }

    protected File getTableFile(String extension) {
        return new File(getRootDir(), getName().toUpperCase() + extension);
    }

    protected File getRootDir() {
        return _dir;
    }

    private File getDataFile() {
        if (_dataFile != null) {
            return _dataFile;
        }

        if ((_fileName.indexOf("\\") > -1) || (_fileName.indexOf("/") > -1) || _fileName.indexOf(File.separator) > -1) {
            // if filename already contains path info, then use as-is
            _dataFile = new File(_fileName);
        } else {
            // the reason why we are using the db dir as data file dir
            // one may have flat files exported from some other system
            // and want to create
            _dataFile = new File(_dbdir, _fileName);
        }
        return _dataFile;
    }

    protected synchronized void renameTableFiles(String oldName, String name, Properties newFileProps) {
        String newDir = null;
        String oldFileName = null;
        File newDirObj = null;
        String newFileName = null;
        // rename table dir
        File olddir = new File(_dbdir, oldName);
        _dir = new File(_dbdir, name);
        olddir.renameTo(_dir);

        // rename table files
        FileUtil.renameFile(_dir, oldName, name, TYPE_FILE_EXT);
        FileUtil.renameFile(_dir, oldName, name, PIDX_FILE_EXT);
        FileUtil.renameFile(_dir, oldName, name, META_FILE_EXT);

        if (newFileProps != null) {
            newDir = newFileProps.getProperty(PARAM_KEY_FILE_DIR);
            newFileName = newFileProps.getProperty(PARAM_KEY_FILE_NAME);
            oldFileName = newFileProps.getProperty(PARAM_KEY_TEMP_FILE_NAME);
        }

        if (newDir != null) {
            newDirObj = new File(newDir);
            FileUtil.renameFile(newDirObj, oldFileName, newFileName);
            _fileName = newDir + newFileName;
        } else {
            FileUtil.renameFile(_dbdir, oldName, name, "." + getDefaultDataFileExtension());
            _fileName = name + "." + getDefaultDataFileExtension();
        }
        context.setProperty(PROP_FILENAME, _fileName);
    }

    protected synchronized void renameTableFiles(String oldName, String name) {
        renameTableFiles(oldName, name, null);
    }

    private WorkbookSettings getWorkbookSettings() {
        WorkbookSettings settings = new WorkbookSettings();
        settings.setDrawingsDisabled(true);
        settings.setAutoFilterDisabled(true);
        settings.setSuppressWarnings(true);
        settings.setNamesDisabled(true);
        settings.setIgnoreBlanks(true);
        settings.setCellValidationDisabled(true);
        settings.setFormulaAdjust(false);
        settings.setPropertySets(false);
        return settings;
    }

    private class SpreadsheetTableOrganizationContext extends BaseTableOrganizationContext {

        public Set getPropertyKeys() {
            Set keys = new HashSet(PROPERTY_KEYS.size());
            keys.addAll(PROPERTY_KEYS);
            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            assertValidPropertyKeys(props);

            _fileName = props.getProperty(PROP_FILENAME);
            if (isNullString(_fileName)) {
                _fileName = getName() + "." + getDefaultDataFileExtension();
            }
            _sheetName = props.getProperty(PROP_SHEET, "Sheet1");

            String validation = props.getProperty(PROP_VALIDATION);
            if (!isNullString(validation)) {
                if ("false".equalsIgnoreCase(validation)) {
                    _validate = false;
                }
            }

            String firstLineHeader = props.getProperty(PROP_ISFIRSTLINEHEADER, "false");
            _isFirstLineHeader = Boolean.valueOf(firstLineHeader).booleanValue();

            if ((props.getProperty(PROP_CREATE_IF_NOT_EXIST) != null) && ("FALSE".equalsIgnoreCase(props.getProperty(PROP_CREATE_IF_NOT_EXIST)))) {
                _isCreateDataFileIfNotExist = false;
            }

            String skipRowsStr = props.getProperty(PROP_ROWSTOSKIP, "0");
            try {
                _rowsToSkip = Integer.parseInt(skipRowsStr);
            } catch (NumberFormatException e) {
                _rowsToSkip = 0;
            }

            String maxFaultStr = props.getProperty(PROP_MAXFAULTS, Long.toString(Long.MAX_VALUE));
            try {
                _maxFaults = Integer.parseInt(maxFaultStr);
            } catch (NumberFormatException e) {
                _maxFaults = Integer.MAX_VALUE;
            }

            // Negative values are meaningless...in this case use Long.MAX_VALUE to
            // represent unlimited fault threshold.
            if (_maxFaults < 0) {
                _maxFaults = Integer.MAX_VALUE;
            }
        }

        public void updateProperties() {
            super.updateProperties();
            
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_SPREADSHEET);
            _props.setProperty(PROP_ISFIRSTLINEHEADER, Boolean.toString(_isFirstLineHeader));
            _props.setProperty(PROP_CREATE_IF_NOT_EXIST, Boolean.toString(_isCreateDataFileIfNotExist));
            _props.setProperty(PROP_FILENAME, _fileName);
            _props.setProperty(PROP_SHEET, _sheetName);
            _props.setProperty(PROP_ROWSTOSKIP, Integer.toString(_rowsToSkip));
            _props.setProperty(PROP_MAXFAULTS, Integer.toString(_maxFaults));
            _props.setProperty(PROP_VALIDATION, String.valueOf(_validate));
        }

        public Set getRequiredPropertyKeys() {
            Set baseRequiredKeys = getBaseRequiredPropertyKeys();
            Set keys = new HashSet(baseRequiredKeys.size() + REQUIRED_KEYS.size());
            keys.addAll(baseRequiredKeys);
            keys.addAll(REQUIRED_KEYS);
            return keys;
        }
    }
}

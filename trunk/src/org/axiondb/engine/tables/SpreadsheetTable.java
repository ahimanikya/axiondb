/*
 * XMLTable.java
 *
 * Created on March 13, 2007, 6:08 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.axiondb.engine.tables;

import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import jxl.CellType;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.BiffException;
import jxl.write.DateTime;
import jxl.write.Label;
import jxl.write.WritableCell;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;
import jxl.write.Number;

import org.apache.commons.collections.primitives.IntCollection;
import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Index;
import org.axiondb.Row;
import org.axiondb.RowCollection;
import org.axiondb.RowIterator;
import org.axiondb.TableFactory;
import org.axiondb.engine.rowiterators.BaseRowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.io.FileUtil;
import org.axiondb.types.CharacterType;
import org.axiondb.types.StringType;

/**
 *
 * @author karthikeyan s
 */
public class SpreadsheetTable extends BaseTable implements ExternalTable {
    
    /** The name of my ".data" file. */
    protected File _dataFile = null;
    
    protected File _dbdir = null;
    
    protected boolean _readOnly = false;
    
    private int _rowCount = -1;
    
    private int _currentRow = -1;
    
    private File _dir;
    
    protected String _fileName;
    
    protected String _sheetName;
    
    private Properties prop = new Properties();
    
    public static final String PROP_FILENAME = "FILENAME";
    
    public static final String PROP_SHEET = "SHEET";
    
    protected static final String META_FILE_EXT = ".META";
    
    protected static final String TYPE_FILE_EXT = ".TYPE";
    
    private SpreadsheetTableOrganizationContext context;
    
    private Sheet sheet;
    
    private Workbook workbook;
    
    private WritableWorkbook writeableWorkbook;
    
    protected static AxionFileSystem FS = new AxionFileSystem();
    
    protected static final int CURRENT_META_VERSION = 3;
    
    private static final Set PROPERTY_KEYS = new HashSet(3);
    
    static {
        PROPERTY_KEYS.add(PROP_FILENAME);
        PROPERTY_KEYS.add(PROP_SHEET);
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
        synchronized(this) {
            try {
                int rowid;
                WritableWorkbook writeableWorkbook = getWriteableWorkbook();
                for (IntIterator iter = rowids.iterator(); iter.hasNext();) {
                    rowid = iter.next();
                    writeableWorkbook.getSheet(_sheetName).removeRow(rowid - 1);
                    _rowCount--;
                }
                writeableWorkbook.write();
                writeableWorkbook.close();
                refreshSheet();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    public void applyInserts(RowCollection rows) throws AxionException {
        synchronized(this) {
            try {
                int colCount = sheet.getColumns();
                writeableWorkbook = getWriteableWorkbook();
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row row = iter.next();
                    RowEvent event = new RowInsertedEvent(this, null, row);
                    WritableSheet writeableSheet = writeableWorkbook.getSheet(_sheetName);
                    writeableSheet.insertRow(_rowCount);
                    for(int i = 0; i < colCount; i++) {
                        String colValue = String.valueOf(row.get(i));
                        Label label = new Label(i, _rowCount, colValue);
                        writeableSheet.addCell(label);
                    }
                    _rowCount++;
                }
                writeableWorkbook.write();
                writeableWorkbook.close();
                refreshSheet();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    public void applyUpdates(RowCollection rows) throws AxionException {
        synchronized(this) {
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
                    for(int i = 0; i < colCount; i++) {
                        Label label = new Label(i, newrow.getIdentifier() - 1, String.valueOf(newrow.get(i)));
                        WritableCell cell = writeableSheet.getWritableCell(i, newrow.getIdentifier() - 1);
                        cell.setCellFormat(cell.getCellFormat());
                        if (cell.getType() == CellType.LABEL) {
                            Label l = (Label) cell;
                            l.setString(String.valueOf(newrow.get(i)));
                        } else if(cell.getType() == CellType.NUMBER) {
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
    }
    
    public Row getRow(int id) throws AxionException {
        Row row;
        try {
            int colCount = sheet.getColumns();
            row = new SimpleRow(id, colCount);
            for(int i = 0; i < colCount; i++) {
                String columnValue = sheet.getCell(i, id - 1).getContents();
                row = trySettingColumn(id, row, i, columnValue);
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
            int _currentId = 0;
            int _currentIndex = 0;
            int _nextId = 1;
            int _nextIndex = 1;
            public Row current() {
                if (!hasCurrent()) {
                    throw new NoSuchElementException("No current row.");
                }
                return _current;
            }
            
            public final int currentIndex() {
                return _currentIndex;
            }
            
            public final boolean hasCurrent() {
                return (null != _current);
            }
            
            public final boolean hasNext() {
                return nextIndex() <= getRowCount();
            }
            
            public final boolean hasPrevious() {
                return nextIndex() > 1;
            }
            
            public Row last() throws AxionException {
                if (isEmpty()) {
                    throw new IllegalStateException("No rows in table.");
                }
                _nextId = _nextIndex = getRowCount();
                previous();
                
                _nextIndex++;
                _nextId++;
                return current();
            }
            
            public Row next() throws AxionException {
                if (!hasNext()) {
                    throw new NoSuchElementException("No next row");
                }
                
                do {
                    _currentId = _nextId++;
                    _current = getRowByOffset(_currentId);
                } while (null == _current);
                _currentIndex = _nextIndex;
                _nextIndex++;
                return _current;
            }
            
            public final int nextIndex() {
                return _nextIndex;
            }
            
            public Row previous() throws AxionException {
                if (!hasPrevious()) {
                    throw new NoSuchElementException("No previous row");
                }
                do {
                    _currentId = (--_nextId);
                    _current = getRowByOffset(_currentId);
                } while (null == _current);
                _nextIndex--;
                _currentIndex = _nextIndex;
                return _current;
            }
            
            public final int previousIndex() {
                return _nextIndex - 1;
            }
            
            public void remove() throws AxionException {
                if (0 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                deleteRow(_current);
                _nextIndex--;
                _currentIndex = 0;
            }
            
            public void reset() {
                _current = null;
                _nextIndex = 1;
                _currentId = 0;
                _currentIndex = 0;
                _nextId = 1;
            }
            
            public final void set(Row row) throws AxionException {
                if (0 == _currentIndex) {
                    throw new IllegalStateException("No current row.");
                }
                updateRow(_current, row);
            }
            
            public final int size() throws AxionException {
                return getRowCount();
            }
            
            public String toString() {
                return "SpreadsheetTable(" + getName() + ")";
            }
            
            private Row setCurrentRow() throws AxionException {
                Row row = getRowByOffset(_currentId);
                if (row != null) {
                    return _current = row;
                }
                throw new IllegalStateException("No valid row at position " + _currentIndex);
            }
        };
    }
    
    public void truncate() throws AxionException {
        try {
            Workbook workbook = Workbook.getWorkbook(getDataFile());
            int index = -1;
            for(int i = 0; i < workbook.getNumberOfSheets(); i++) {
                if(workbook.getSheet(i).getName().equals(_sheetName)) {
                    index = i;
                    break;
                }
            }
            workbook.close();
            
            // implement remove sheet and insert sheet at index.
            WritableWorkbook writeableWorkbook = getWriteableWorkbook();
            writeableWorkbook.removeSheet(index);
            writeableWorkbook.createSheet(_sheetName, index);
            writeableWorkbook.write();
            writeableWorkbook.close();
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }
    
    public boolean loadExternalTable(Properties prop) throws AxionException {
        try {
            if(context == null) {
                context = new SpreadsheetTableOrganizationContext();
            }
            context.readOrSetDefaultProperties(prop);
            context.updateProperties();
            
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
        if (!FileUtil.delete(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table " + getName());
        }
    }   
    
    public Properties getTableProperties() {
        prop.clear();
        prop.put(PROP_LOADTYPE, ExternalTableFactory.TYPE_SPREADSHEET);
        prop.put(PROP_FILENAME, _dataFile.getAbsolutePath());
        prop.put(PROP_SHEET, _sheetName);
        return prop;
    }
    
    public void remount() throws AxionException {
        try {
            Workbook workbook = Workbook.getWorkbook(getDataFile(), getWorkbookSettings());
            sheet = workbook.getSheet(_sheetName);
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }
    
    protected boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
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
        
        if(version < 3) {
            // change col name to upper if required
            for(int i = 0, I = getColumnCount(); i < I; i++) {
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
    
    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        try {
            _fileName = in.readUTF();
            _sheetName = in.readUTF();
            
            context = new SpreadsheetTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            if(!getDataFile().exists()) {
                throw new AxionException("Source file does not exist.");
            }
            initializeTable();
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
            if (_fileName != null ) {
                out.writeUTF(_fileName);
                out.writeUTF(_sheetName);
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
    
    protected Row getRowByOffset(int idToAssign) throws AxionException {
        Row row;
        synchronized(this) {
            try {
                int colCount = sheet.getColumns();
                row = new SimpleRow(idToAssign, colCount);
                for(int i = 1; i <= colCount; i++) {
                    String columnValue = sheet.getCell(i - 1, idToAssign - 1).getContents();
                    row = trySettingColumn(idToAssign, row, i-1, columnValue);
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
    
    private Row trySettingColumn(int idToAssign, Row row, int i, String colValue) throws AxionException {
        DataType columnDataType = getColumn(i).getDataType();
        
        colValue = evaluateForNull(colValue, columnDataType);
        if (colValue == null) {
            row.set(i, null);
        } else {
            Object val = columnDataType.convert(colValue);
            row.set(i, val);
        }
        
        return row;
    }
    
    private String evaluateForNull(String colValue,  DataType datatype) {
        if(null == colValue ){
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
        if(workbook == null) {
            refreshSheet();
        }
        writeableWorkbook = Workbook.createWorkbook(getDataFile(), workbook, getWorkbookSettings());
        return writeableWorkbook;
    }
    
    protected void initializeTable() throws AxionException {
        try {
            workbook = Workbook.getWorkbook(getDataFile(), getWorkbookSettings());
            sheet = workbook.getSheet(_sheetName);
            _rowCount = sheet.getRows();
            _currentRow = 0;
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }
    
    private void refreshSheet() throws IOException, BiffException {
        if(workbook != null) {
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
        return _dataFile;
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
            _dataFile = new File(_fileName);
            if (isNullString(_fileName)) {
                _fileName = getName() + "." + getDefaultDataFileExtension();
            }
            _sheetName = props.getProperty(PROP_SHEET);
        }
        
        public void updateProperties() {
            super.updateProperties();
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_SPREADSHEET);
            _props.setProperty(PROP_FILENAME, _fileName);
            _props.setProperty(PROP_SHEET, _sheetName);
        }
        
        public Set getRequiredPropertyKeys() {
            Set keys = new HashSet(PROPERTY_KEYS.size());
            keys.addAll(PROPERTY_KEYS);
            return keys;
        }
    }
}

/*
 * WebRowSetTable.java
 *
 * Created on February 26, 2007, 2:35 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.axiondb.engine.tables;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import javax.sql.rowset.WebRowSet;
import com.sun.rowset.WebRowSetImpl;

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
public class WebRowSetTable extends BaseTable implements ExternalTable {
    
    /** The name of my ".data" file. */
    protected File _dataFile = null;
    protected File _dbdir = null;
    
    protected boolean _readOnly = false;
    
    private File _dir;
    
    protected String _fileName;
    
    private int _rowCount = -1;
    
    private int _currentRow = -1;    
    
    private Properties prop = new Properties();
    
    public static final String PROP_FILENAME = "FILENAME";
    
    protected static final String META_FILE_EXT = ".META";
    
    protected static final String TYPE_FILE_EXT = ".TYPE";
    
    private WebRowSetTableOrganizationContext context;
    
    protected static AxionFileSystem FS = new AxionFileSystem();
    
    protected static final int CURRENT_META_VERSION = 3;
    
    private static final Set PROPERTY_KEYS = new HashSet(2);
    
    static {
        PROPERTY_KEYS.add(PROP_FILENAME);
        PROPERTY_KEYS.add(ExternalTable.PROP_LOADTYPE);
    }
    
    private WebRowSet xmlRowSet;
    
    /**
     * Creates a new instance of WebRowSetTable
     */
    public WebRowSetTable(String name, Database db) throws AxionException {
        super(name);
        setType(ExternalTable.WEBROWSET_TABLE_TYPE);
        _dbdir = db.getDBDirectory();
        _readOnly = db.isReadOnly();
        createOrLoadTableFiles(name, db, new WebRowSetTableLoader());
    }
    
    public void applyDeletes(IntCollection rowids) throws AxionException {
        synchronized(this) {
            try {
                int rowid;
                for (IntIterator iter = rowids.iterator(); iter.hasNext();) {
                    rowid = iter.next();
                    xmlRowSet.absolute(rowid);
                    xmlRowSet.deleteRow();
                    _rowCount--;
                }
                xmlRowSet.moveToCurrentRow();
                writeToFile();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    public void applyInserts(RowCollection rows) throws AxionException {
        synchronized(this) {
            try {
                int colCount = xmlRowSet.getMetaData().getColumnCount();
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row row = iter.next();
                    RowEvent event = new RowInsertedEvent(this, null, row);
                    xmlRowSet.moveToInsertRow();
                    for(int i = 1; i <= colCount; i++) {
                        String colValue = String.valueOf(row.get(i-1));
                        xmlRowSet.updateObject(i, colValue);
                    }
                    xmlRowSet.insertRow();
                    xmlRowSet.moveToCurrentRow();
                    _rowCount++;
                }
                writeToFile();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    public void applyUpdates(RowCollection rows) throws AxionException {
        synchronized(this) {
            try {
                int colCount = xmlRowSet.getMetaData().getColumnCount();
                
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row newrow = iter.next();
                    Row oldrow = getRow(newrow.getIdentifier());
                    if (oldrow != null) {
                        RowEvent event = new RowUpdatedEvent(this, oldrow, newrow);
                    }
                    xmlRowSet.absolute(newrow.getIdentifier());
                    for(int i = 1; i <= colCount; i++) {
                        xmlRowSet.updateObject(i, String.valueOf(newrow.get(i - 1)));
                    }
                    xmlRowSet.updateRow();
                    xmlRowSet.moveToCurrentRow();
                }
                writeToFile();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    
    public void freeRowId(int id) {
    }
    
    @Override
    public void drop() throws AxionException {
        super.drop();  
        if(xmlRowSet != null) {
            try {
                xmlRowSet.close();
            } catch (SQLException ex) {
                throw new AxionException("Unable to close the resultset.");
            }
        }
        if (!FileUtil.delete(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table " + getName());
        }
    }       
    
    private synchronized void writeToFile() throws SQLException, IOException {
        StringWriter writer = new StringWriter();
        xmlRowSet.writeXml(writer);
        String str = replaceTempRows(writer).toString();
        BufferedWriter bw = new BufferedWriter(new FileWriter(getDataFile()));
        bw.write(str, 0, str.length());
        bw.flush();
        bw.close();
        xmlRowSet.close();
        
        // refresh the resultset.
        xmlRowSet = new WebRowSetImpl();
        xmlRowSet.readXml(new FileReader(getDataFile()));
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
            int colCount = xmlRowSet.getMetaData().getColumnCount();
            row = new SimpleRow(id, colCount);
            xmlRowSet.absolute(id);
            for(int i = 0; i < colCount; i++) {
                String columnValue = String.valueOf(xmlRowSet.getObject(i + 1));
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
                return "XMLTable(" + getName() + ")";
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
            if(xmlRowSet == null) {
                xmlRowSet = new WebRowSetImpl();
                xmlRowSet.readXml(new FileReader(getDataFile()));
            }
            
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
        
    }
    
    public boolean loadExternalTable(Properties prop) throws AxionException {
        try {
            if(context == null) {
                context = new WebRowSetTableOrganizationContext();
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
    
    public Properties getTableProperties() {
        prop.clear();
        prop.put(PROP_LOADTYPE, "WEBROWSET");
        prop.put(PROP_FILENAME, _dataFile.getAbsolutePath());
        return prop;
    }
    
    public void remount() throws AxionException {
        try {
            xmlRowSet = new WebRowSetImpl();
            xmlRowSet.readXml(new FileReader(getDataFile()));
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
        
    }
    
    private void createOrLoadTableFiles(String name, Database db, TableFactory factory) throws AxionException {
        synchronized (WebRowSetTable.class) {
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
    
    protected void initializeTable() throws AxionException {
        try {
            if(xmlRowSet == null) {
                xmlRowSet = new WebRowSetImpl();
            }                
            xmlRowSet.readXml(new FileReader(getDataFile()));   
            _rowCount = xmlRowSet.size();
            _currentRow = 0;
        } catch (Exception e) {
            throw new AxionException(e);
        }
    }
    
    protected Row getRowByOffset(int idToAssign) throws AxionException {
        Row row;
        synchronized(this) {
            try {
                xmlRowSet.absolute(idToAssign);
                int colCount = xmlRowSet.getMetaData().getColumnCount();
                row = new SimpleRow(idToAssign, colCount);
                for(int i = 1; i <= colCount; i++) {
                    String columnValue = String.valueOf(xmlRowSet.getObject(i));
                    row = trySettingColumn(idToAssign, row, i-1, columnValue);
                }
            } catch (Exception e) {
                if (e instanceof AxionException) {
                    throw (AxionException) e;
                }
                throw new AxionException(e);
            }
        }
        _currentRow = idToAssign;
        return row;
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
    
    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        try {
            _fileName = in.readUTF();
            context = new WebRowSetTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            initializeTable();
        } catch (IOException ioex) {
            throw new AxionException("Unable to parse meta file for table " + getName(), ioex);
        }
    }
    
    private void parseV1MetaFile(ObjectInputStream in, Database db) throws AxionException, IOException, ClassNotFoundException {
        readColumns(in);
        readConstraints(in, db);
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
    
    public void addColumn(Column col, boolean metaUpdateNeeded) throws AxionException {
        super.addColumn(col);
        if (metaUpdateNeeded) {
            writeMetaFile();
        }
    }
    
    private File getDataFile() {
        return _dataFile;
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
    
    private class WebRowSetTableOrganizationContext extends BaseTableOrganizationContext {
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
        }
        
        public void updateProperties() {
            super.updateProperties();
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_WEBROWSET);
            _props.setProperty(PROP_FILENAME, _fileName);
        }
        
        public Set getRequiredPropertyKeys() {
            Set keys = new HashSet(PROPERTY_KEYS.size());
            keys.addAll(PROPERTY_KEYS);
            return keys;
        }
    }
    
    protected boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }
    
    private String getDefaultDataFileExtension() {
        return "xml";
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
    
    protected File getTableFile(String extension) {
        return new File(getRootDir(), getName().toUpperCase() + extension);
    }
    
    protected File getRootDir() {
        return _dir;
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
    
    protected void writeTableProperties(ObjectOutputStream out) throws AxionException {
        try {
            if (_fileName != null ) {
                out.writeUTF(_fileName);
            }
        } catch (IOException ioex) {
            throw new AxionException("Unable to write meta file for table " + getName(), ioex);
        }
    }
    
    private StringBuffer replaceTempRows(StringWriter writer) {
        StringBuffer buf = new StringBuffer(writer.toString());
        
        // replace all <deleteRow>
        buf  = deleteMarkedRows(buf);
        // replace all <modifyRow>
        buf = replaceModifySequence(buf);
        // replace all <insertRow>
        buf = replaceInsertSequence(buf);
        return buf;
    }
    
    private StringBuffer replaceInsertSequence(StringBuffer buf) {
        boolean shouldSearch = false;
        String startString = "<insertRow>";
        String endString = "</insertRow>";
        int startFrom = 0;
        while(buf.indexOf(startString, startFrom) != -1) {
            shouldSearch = true;
            int start = buf.indexOf(startString);
            int end = start + startString.length();
            buf.replace(start, end, "<currentRow>");
            startFrom = end;
        }
        
        // search for ending tag only if starting tag is present.
        if(shouldSearch) {
            startFrom = 0;
            while(buf.indexOf(endString, startFrom) != -1) {
                int start = buf.indexOf(endString);
                int end = start + endString.length();
                buf.replace(start, end, "</currentRow>");
                startFrom = end;
            }
        }
        return buf;
    }
    
    /**
     *
     * @param buf
     * @return
     */
    private StringBuffer replaceModifySequence(StringBuffer buf) {
        boolean shouldSearch = false;
        
        String startString = "<currentRow>";
        String endString = "</currentRow>";
        String colString = "<columnValue>";
        String colStringEnd = "</columnValue>";
        String updateString = "<updateValue>";
        String updateStringEnd = "</updateValue>";
        String altUpdateString = "<updateRow>";
        String altUpdateStringEnd = "</updateRow>";
        
        int startFrom = 0;
        while(buf.indexOf(startString, startFrom) != -1) {
            
            int start = buf.indexOf(startString, startFrom);
            int end = start + startString.length();
            int endValue = buf.indexOf(endString, end);
            
            //check if updateValue/updaterow is present. If present, do processing.
            int check = buf.indexOf(updateString, start) == -1?
                buf.indexOf(altUpdateString, start): buf.indexOf(updateString, start);
            if(check != -1 && check < endValue) {
                int startFromColValue = end;
                
                //remove all columnValue tags.
                while(buf.indexOf(colString, startFromColValue) != -1 &&
                        startFromColValue < endValue) {
                    int colStart = buf.indexOf(colString, startFromColValue);
                    int colEnd = buf.indexOf(colStringEnd, colStart) + colStringEnd.length();
                    buf.delete(colStart, colEnd);
                    startFromColValue = colEnd;
                    endValue = buf.indexOf(endString, end);
                }
                
                endValue = buf.indexOf(endString, end);
                
                startFromColValue = end;
                while(buf.indexOf("updateValue", startFromColValue) != -1 ||
                        buf.indexOf("updateRow", startFromColValue) != -1) {
                    
                    
                    int startCol = -1;
                    boolean isAlt = false;
                    if(buf.indexOf("updateValue", startFromColValue) != -1) {
                        startCol = buf.indexOf("updateValue", startFromColValue);
                    } else {
                        startCol = buf.indexOf("updateRow", startFromColValue);
                        isAlt = true;
                    }
                    if(startCol >= endValue) break;
                    
                    int endCol = -1;
                    if(isAlt) {
                        endCol = startCol + "updateRow".length();
                    } else {
                        endCol = startCol + "updateValue".length();
                    }
                    buf.replace(startCol, endCol, "columnValue");
                    startFromColValue = startCol;
                    endValue = buf.indexOf(endString, end);
                }
            }
            endValue = buf.indexOf(endString, end);
            startFrom = endValue;
        }
        
        return buf;
    }
    
    private StringBuffer deleteMarkedRows(StringBuffer buf) {
        int startFrom = 0;
        while(buf.indexOf("<deleteRow>", startFrom) != -1) {
            int start = buf.indexOf("<deleteRow>");
            int end = buf.indexOf("</deleteRow>") + "</deleteRow>".length();
            buf.delete(start, end);
            startFrom = end;
        }
        return buf;
    }
}

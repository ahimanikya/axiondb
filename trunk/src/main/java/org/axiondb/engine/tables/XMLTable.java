/*
 * XMLTable.java
 *
 * Created on March 13, 2007, 6:08 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */
package org.axiondb.engine.tables;

import com.sun.org.apache.xml.internal.serialize.OutputFormat;
import com.sun.org.apache.xml.internal.serialize.XMLSerializer;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;
import java.net.URISyntaxException;
import java.net.URI;
import java.util.Iterator;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.stream.StreamFilter;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.XMLStreamWriter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import org.apache.commons.collections.primitives.IntIterator;
import org.axiondb.event.RowEvent;
import org.axiondb.event.RowInsertedEvent;
import org.axiondb.event.RowUpdatedEvent;
import org.axiondb.io.FileUtil;

import org.apache.commons.collections.primitives.IntCollection;
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
import org.axiondb.engine.rowiterators.BaseRowIterator;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.AxionFileSystem;
import org.axiondb.types.CharacterType;
import org.axiondb.types.StringType;
import org.axiondb.util.ExceptionConverter;

/**
 *
 * @author karthikeyan s
 * @author jawed
 * 
 * CREATE EXTERNAL TABLE IF NOT EXISTS XML1 ("empid" varchar(15), "name" varchar(30)) 
 * ORGANIZATION (LOADTYPE='XML', FILENAME='C:\temp\X1.xml',rowname='row', type='readwrite')
 */
public class XMLTable extends BaseTable implements ExternalTable {

    /** The name of my ".data" file. */
    protected File _dataFile = null;
    protected URI _uri = null;
    protected File _dbdir = null;
    protected boolean _readOnly = false;
    private File _dir;
    protected String _fileName;
    private String _rowName;
    private int _rowCount = -1;
    private int _currentRow = -1;
    protected int _rowsToSkip = 0;
    public static final String PROP_FILENAME = "FILENAME";
    public static final String PROP_ROWNAME = "ROWNAME";
    public static final String PROP_ROWSTOSKIP = "ROWSTOSKIP";
    public static final String PROP_VALIDATION = "VALIDATION";
    public static final String PROP_MAXFAULTS = "MAXFAULTS";
    public static final String PROP_READONLY = "TYPE";
    protected static final String PIDX_FILE_EXT = ".PIDX";
    protected static final String META_FILE_EXT = ".META";
    protected static final String TYPE_FILE_EXT = ".TYPE";
    protected static final String INDICES_DIR_NAME = "INDICES";
    private final String READWRITE_STR = "READWRITE";
    private final String READONLY_STR = "READONLY";
    private String _readOnlyStatus;
    private boolean _isCreateDataFileIfNotExist = true;
    protected boolean _isFirstLineHeader = false;
    private boolean _validate = true;
    private File _indexRootDir = null;
    private AxionFileSystem.PidxList _pidx = null;
    private XMLTableOrganizationContext context;
    private TransformerFactory tFactory;
    private Transformer transformer;
    private XMLStreamReader xmlStreamReader;
    private XMLInputFactory xmlInputFactory;
    private DocumentBuilderFactory factory;
    private Document document;
    private DocumentBuilder builder;
    private Element root;
    protected static final long INVALID_OFFSET = Long.MAX_VALUE;
    protected int _maxFaults = Integer.MAX_VALUE;
    protected static AxionFileSystem FS = new AxionFileSystem();
    protected static final int CURRENT_META_VERSION = 3;
    private static final Set REQUIRED_KEYS = new HashSet(1);
    private static final Set PROPERTY_KEYS = new HashSet(4);

    static {
        PROPERTY_KEYS.add(PROP_FILENAME);
        PROPERTY_KEYS.add(PROP_VALIDATION);
        PROPERTY_KEYS.add(PROP_CREATE_IF_NOT_EXIST);
        PROPERTY_KEYS.add(PROP_ROWNAME);
        PROPERTY_KEYS.add(PROP_READONLY);
        PROPERTY_KEYS.add(PROP_ROWSTOSKIP);
        PROPERTY_KEYS.add(PROP_MAXFAULTS);
        PROPERTY_KEYS.add(ExternalTable.PROP_LOADTYPE);
    }

    /** Creates a new instance of XMLTable */
    public XMLTable(String name, Database db) throws AxionException {
        super(name);
        setType(ExternalTable.XML_TABLE_TYPE);
        _dbdir = db.getDBDirectory();
        _readOnly = db.isReadOnly();
        createOrLoadTableFiles(name, db, new XMLTableLoader());
    }

    public void applyDeletes(IntCollection rowids) throws AxionException {
        if (_readOnlyStatus.equalsIgnoreCase(READONLY_STR)) {
            throw new AxionException("Operation not supported");
        }
        synchronized (this) {
            try {
                int rowid;
                for (IntIterator iter = rowids.iterator(); iter.hasNext();) {
                    rowid = iter.next();
                    //root.removeChild(getRowNodeForId(rowid, root));
                    _pidx.set(rowid, INVALID_OFFSET);
                    _rowCount--;
                }
//                document.getDocumentElement().normalize();
//                writeDomToFile();
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
        if (_readOnlyStatus.equalsIgnoreCase(READONLY_STR)) {
            throw new AxionException("Operation not supported");
        }
        synchronized (this) {
            try {
                int colCount = getColumnCount();
                while (rows.hasNext()) {
                    Row row = rows.next();
                    Node rowElem = document.createElement(_rowName);
                    root.appendChild(rowElem);
                    RowEvent event = new RowInsertedEvent(this, null, row);
                    for (int i = 0; i < colCount; i++) {
                        Node columnElem = document.createElement(getColumn(i).getName());
                        rowElem.appendChild(columnElem);
                        String colValue = String.valueOf(row.get(i));
                        if (colValue.equalsIgnoreCase("null")) {
                            colValue = "";
                        }
                        columnElem.appendChild(document.createTextNode(colValue));
                    }
                    _pidx.add(_rowCount);
                    _rowCount++;
                }
                _pidx.flush();
                document.getDocumentElement().normalize();
                writeDomToFile();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }

    public void applyUpdates(RowCollection rows) throws AxionException {
        if (_readOnlyStatus.equalsIgnoreCase(READONLY_STR)) {
            throw new AxionException("Operation not supported");
        }
        synchronized (this) {
            try {
                int colCount = getColumnCount();
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row newrow = iter.next();
                    Row oldrow = getRow(newrow.getIdentifier());
                    if (oldrow != null) {
                        RowEvent event = new RowUpdatedEvent(this, oldrow, newrow);
                    }
                    Element rowElem = (Element) getRowNodeForId(newrow.getIdentifier(), root);
                    for (int i = 0; i < colCount; i++) {
                        Node colNode = rowElem.getElementsByTagName(getColumn(i).getName()).item(0);
                        NodeList colChildren = colNode.getChildNodes();
                        for (int j = 0; j < colChildren.getLength(); j++) {
                            Node node = colChildren.item(j);
                            if (node.getNodeType() == Node.TEXT_NODE) {
                                node.setNodeValue(String.valueOf(newrow.get(i)));
                                break;
                            }
                        }
                    }
                }
                document.getDocumentElement().normalize();
                writeDomToFile();
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
        for (int i = 0,  I = getPidxList().size(); i < I; i++) {
            long ptr = getPidxList().get(i);
            if (ptr != INVALID_OFFSET) {
                row = getRow((int) ptr + 1);
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

    private void saveIndex(Index index) throws AxionException {
        File dataDir = new File(_indexRootDir, index.getName());
        index.save(dataDir);
    }

    public void removeIndex(Index index) throws AxionException {
        super.removeIndex(index);
        File indexdir = new File(_indexRootDir, index.getName());
        if (!FileUtil.delete(indexdir)) {
            throw new AxionException("Unable to delete \"" + indexdir + "\" during remove index " + index.getName());
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

    protected Row getRowByOffset(int idToAssign, long ptr) throws AxionException {
        Row row;
        synchronized (this) {
            try {
                int colCount = getColumnCount();
                row = new SimpleRow(idToAssign, colCount);
                LinkedList<String> columns = getColumnValues((int) ptr + 1);
                for (int i = 1; i <= colCount; i++) {
                    String columnValue = columns.get(i - 1);
                    row = trySettingColumn(row, i - 1, columnValue);
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

    public Row getRow(int id) throws AxionException {
        LinkedList<String> columns;
        Row row;
        try {
            int colCount = getColumnCount();
            row = new SimpleRow(id, colCount);
            columns = getColumnValues(id);
            for (int i = 1; i <= colCount; i++) {
                String columnValue = columns.get(i - 1);
                row = trySettingColumn(row, i - 1, columnValue);
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
                return "XMLTable(" + getName() + ")";
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
        closeFiles();
        getDataFile().delete();
        try {
            getDataFile().createNewFile();
            XMLStreamWriter writer = XMLOutputFactory.newInstance().createXMLStreamWriter(
                    new FileOutputStream(getDataFile()));
            writer.writeStartDocument("1.0");
            writer.writeStartElement(getName());
            writer.writeEndDocument();
            writer.flush();
            writer.close();
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }

    public void shutdown() throws AxionException {
        closeFiles();
    }

    @Override
    public void drop() throws AxionException {
        super.drop();
        closeFiles();
        if (!FileUtil.delete(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table " + getName());
        }
    }

    protected void closeFiles() throws AxionException {
        try {
            if (xmlStreamReader != null) {
                xmlStreamReader.close();
            }
        } catch (XMLStreamException ex) {
            throw new AxionException(ex);
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

    protected void initializeRowCount() throws AxionException {
        _rowCount = 0;
        for (int i = 0,  I = getPidxList().size(); i < I; i++) {
            long ptr = getPidxList().get(i);
            if (ptr != INVALID_OFFSET) {
                _rowCount++;
            }
        }
    }

    private void createRootElement(File dataFile) throws AxionException {
        try {
            //get an instance of factory
            DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
            Document dom = null;
            try {
                //get an instance of builder
                DocumentBuilder db = dbf.newDocumentBuilder();

                //create an instance of DOM
                dom = db.newDocument();
            } catch (ParserConfigurationException pce) {
                throw new AxionException("Error while trying to instantiate DocumentBuilder ", pce);
            }

            Element rootEle = dom.createElement("table");
            dom.appendChild(rootEle);
            OutputFormat format = new OutputFormat(dom);
            format.setIndenting(true);
            XMLSerializer serializer = new XMLSerializer(new FileOutputStream(dataFile), format);
            serializer.serialize(dom);
        } catch (IOException ie) {
            ie.printStackTrace();
        }
    }

    protected void createOrLoadDataFile() throws AxionException {
        createOrLoadDataFile(_isCreateDataFileIfNotExist);
    }

    protected void createOrLoadDataFile(boolean createNewDataFile) throws AxionException {
        if (!createNewDataFile && !getDataFile().exists()) {
            throw new AxionException("Data file \"" + getDataFile() + "\" for table " + getName() + " not found.");
        }

        try {
            if (getDataFile().createNewFile()) {
                createRootElement(getDataFile());
            //writeHeader(getOutputStream());
            } else {
                parseUsingDOM();
            }
        } catch (Exception ioex) {
            throw new AxionException("Unable to create data file \"" + getDataFile() + "\" for table " + getName() + ".", ioex);
        }
    }

    public boolean loadExternalTable(Properties prop) throws AxionException {
        try {
            if (context == null) {
                context = new XMLTableOrganizationContext();
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

    public void remount() throws AxionException {
    }

    protected boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }

    private String getDefaultDataFileExtension() {
        return "xml";
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

    private void createOrLoadTableFiles(String name, Database db, TableFactory factory) throws AxionException {
        synchronized (XMLTable.class) {
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
            for (int i = 0,  I = getColumnCount(); i < I; i++) {
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
            _fileName = in.readUTF();
            _isCreateDataFileIfNotExist = Boolean.valueOf(in.readUTF()).booleanValue();
            _rowName = in.readUTF();
            _readOnlyStatus = in.readUTF();
            _rowsToSkip = in.readInt();
            _maxFaults = in.readInt();
            _validate = Boolean.valueOf(in.readUTF()).booleanValue();

            context = new XMLTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            if (getDataFile().getPath().startsWith("file:")) {
                ;
            } else if (getDataFile().exists()) {
                ;
            } else {
                throw new AxionException("Source file does not exist.");
            }
            createOrLoadDataFile();
        //initializeTable();
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
                out.writeUTF(_fileName);
                out.writeUTF(Boolean.toString(_isCreateDataFileIfNotExist));
                out.writeUTF(_rowName);
                out.writeUTF(_readOnlyStatus);
                out.writeInt(_rowsToSkip);
                out.writeInt(_maxFaults);
                out.writeUTF(Boolean.toString(_validate));
            }
        } catch (IOException ioex) {
            throw new AxionException("Unable to write meta file for table " + getName(), ioex);
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

    private URI getURI() throws URISyntaxException {
        return _uri;
    }

    protected void initializeTable() throws AxionException {
        try {
            parseXMLFile();
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }

    private void parseXMLFile() throws XMLStreamException, FileNotFoundException,
            ParserConfigurationException, SAXException, IOException, AxionException {
        Row row;
        _rowCount = 0;
        _currentRow = 0;
        int faultCount = 0;
        int colCount = getColumnCount();
        int rowPos = ignoreRowsToSkip();
        AxionFileSystem.PidxList pidx = getPidxList();
        if (_readOnlyStatus.equalsIgnoreCase(READONLY_STR)) {
            parseUsingStax();

            int event = -1;
            // get Row count.
            while (xmlStreamReader.hasNext()) {
                event = xmlStreamReader.next();
                if (event == XMLStreamConstants.START_ELEMENT && xmlStreamReader.getLocalName().equals(_rowName)) {
                    pidx.add(rowPos);
                    if (_validate) {
                        try {
                            row = new org.axiondb.engine.rows.SimpleRow(rowPos, colCount);
                            java.util.LinkedList<java.lang.String> columns = getColumnValues(rowPos + 1);
                            for (int i = 1; i <= colCount; i++) {
                                java.lang.String columnValue = columns.get(i - 1);
                                row = trySettingColumn(row, i - 1, columnValue);
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
                }
            }
            parseUsingStax();
        } else {
            parseUsingDOM();
            if (root != null) {
                NodeList rows = root.getElementsByTagName(_rowName);
                for(int i = 0, I = rows.getLength(); i < I; i++) {
                    if(rowPos == I){
                        break;
                    }
                    Node rowNode = rows.item(i);
                    if (rowNode.getNodeType() == Node.ELEMENT_NODE) {
                        pidx.add(rowPos);
                        if (_validate) {
                            try {
                                row = new org.axiondb.engine.rows.SimpleRow(rowPos, colCount);
                                java.util.LinkedList<java.lang.String> columns = getColumnValues(rowPos + 1);
                                for (int j = 1; j <= colCount; j++) {
                                    java.lang.String columnValue = columns.get(j - 1);
                                    row = trySettingColumn(row, j - 1, columnValue);
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
                    }
                }
            }
        }
        pidx.flush();
    }

    private void writeDomToFile() throws TransformerConfigurationException,
            TransformerException, ParserConfigurationException, SAXException, IOException, XMLStreamException {
        // Use a Transformer for output
        if (tFactory == null) {
            tFactory =
                    TransformerFactory.newInstance();
            transformer = tFactory.newTransformer();
        }
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        DOMSource source = new DOMSource(document);
        try {
            StreamResult result = null;
            if (getDataFile().exists()) {
                result = new StreamResult(getDataFile());
            } else {
                result = new StreamResult(new File(getURI()));
            }
            transformer.transform(source, result);
        } catch (URISyntaxException e) {

        }
    }

    private Node getRowNodeForId(int idToAssign, Element root) throws AxionException {
        try {
            XPathFactory factory = XPathFactory.newInstance();
            XPath xPath = factory.newXPath();
            String xpathExpString = "//" + _rowName + "[" + String.valueOf(idToAssign) + "]";
            XPathExpression xPathExpression = xPath.compile(xpathExpString);
            return (Node) xPathExpression.evaluate(root, XPathConstants.NODE);
        } catch (XPathExpressionException ex) {
            throw new AxionException(ex);
        }
    }

    private LinkedList<String> getColumnValues(int rowId) throws AxionException {
        LinkedList<String> columns = new LinkedList<String>();
        if (_readOnlyStatus.equalsIgnoreCase(READWRITE_STR)) {
            Element row = (Element) getRowNodeForId(rowId, root);
            for (int i = 0; i < getColumnCount(); i++) {
                Element column = (Element) row.getElementsByTagName(getColumn(i).getName()).item(0);
                if (column != null) {
                    NodeList colElems = column.getChildNodes();
                    for (int j = 0; j < colElems.getLength(); j++) {
                        Node nd = colElems.item(j);
                        if (nd.getNodeType() == Node.TEXT_NODE) {
                            columns.add(nd.getTextContent());
                            break;
                        }
                    }
                } else {
                    columns.add("");
                }
            }
        } else {
            // get columns using Stax parser
            columns = readUsingStax(rowId);
        }
        return columns;
    }

    private void parseUsingDOM() throws ParserConfigurationException, SAXException, IOException {
        try {
            factory = DocumentBuilderFactory.newInstance();
            builder = factory.newDocumentBuilder();
            if (getDataFile().exists()) {
                document = builder.parse(getDataFile());
            } else {
                document = builder.parse(getURI().toURL().openStream());
            }
            root = document.getDocumentElement();
        } catch (URISyntaxException e) {
        }
    }

    private void parseUsingStax() throws FileNotFoundException, XMLStreamException {
        if (xmlStreamReader != null) {
            xmlStreamReader.close();
        }
        FileInputStream fileInputStream = new FileInputStream(getDataFile());
        if (xmlInputFactory == null) {
            xmlInputFactory = XMLInputFactory.newInstance();
            xmlInputFactory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES, Boolean.FALSE);
            xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, Boolean.FALSE);
            xmlInputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE, Boolean.FALSE);
            xmlInputFactory.setProperty(XMLInputFactory.IS_COALESCING, Boolean.TRUE);
        }
        xmlStreamReader = xmlInputFactory.createFilteredReader(xmlInputFactory.createXMLStreamReader(fileInputStream), new XMLElementFilter());
    }

    private LinkedList<String> readUsingStax(int rowId) throws AxionException {
        LinkedList<String> columns = new LinkedList<String>();
        int rowCount = _currentRow;
        int event = -1;

        try {
            if (_currentRow > rowId) {
                // read From First
                rowCount = 0;
                parseUsingStax();
            }

            while (xmlStreamReader.hasNext()) {

                event = xmlStreamReader.next();

                if (event == XMLStreamConstants.START_ELEMENT &&
                        xmlStreamReader.getLocalName().equals(_rowName)) {
                    rowCount++;
                }
                if (rowCount == rowId) {
                    break;
                }
            }
            for (int i = 0; i < getColumnCount(); i++) {
                // move to the column element.
                event = xmlStreamReader.next();
                while (!(event == XMLStreamConstants.START_ELEMENT &&
                        xmlStreamReader.getLocalName().equals(getColumn(i).getName()))) {
                    event = xmlStreamReader.next();
                }
                String elemText = xmlStreamReader.getElementText();
                elemText = (elemText == null) ? "" : elemText;
                columns.add(elemText);
            }
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
        return columns;
    }

    private class XMLTableOrganizationContext extends BaseTableOrganizationContext {

        public Set getPropertyKeys() {
            Set keys = new HashSet(PROPERTY_KEYS.size());
            keys.addAll(PROPERTY_KEYS);
            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            assertValidPropertyKeys(props);

            _fileName = props.getProperty(PROP_FILENAME);
            _dataFile = new File(_fileName);
            if (!_dataFile.exists()) {
                try {
                    _uri = new URI(_fileName);
                } catch (URISyntaxException e) {

                }
            }

            if ((props.getProperty(PROP_CREATE_IF_NOT_EXIST) != null) && ("FALSE".equalsIgnoreCase(props.getProperty(PROP_CREATE_IF_NOT_EXIST)))) {
                _isCreateDataFileIfNotExist = false;
            }

            _rowName = props.getProperty(PROP_ROWNAME, "row");

            _readOnlyStatus = props.getProperty(PROP_READONLY, READWRITE_STR);

            if (isNullString(_fileName)) {
                _fileName = getName() + "." + getDefaultDataFileExtension();
            }

            String validation = props.getProperty(PROP_VALIDATION);
            if (!isNullString(validation)) {
                if ("false".equalsIgnoreCase(validation)) {
                    _validate = false;
                }
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
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_XML);
            _props.setProperty(PROP_FILENAME, _fileName);
            _props.setProperty(PROP_VALIDATION, String.valueOf(_validate));
            _props.setProperty(PROP_CREATE_IF_NOT_EXIST, Boolean.toString(_isCreateDataFileIfNotExist));
            _props.setProperty(PROP_ROWNAME, _rowName);
            _props.setProperty(PROP_READONLY, _readOnlyStatus);
            _props.setProperty(PROP_ROWSTOSKIP, Integer.toString(_rowsToSkip));
            _props.setProperty(PROP_MAXFAULTS, Integer.toString(_maxFaults));
        }

        public Set getRequiredPropertyKeys() {
//            Set keys = new HashSet(PROPERTY_KEYS.size());
//            keys.addAll(PROPERTY_KEYS);
            Set baseRequiredKeys = getBaseRequiredPropertyKeys();
            Set keys = new HashSet(baseRequiredKeys.size() + REQUIRED_KEYS.size());
            keys.addAll(baseRequiredKeys);
            keys.addAll(REQUIRED_KEYS);
            return keys;
        }
    }

    private static class XMLElementFilter implements StreamFilter {

        public boolean accept(XMLStreamReader reader) {
            if (reader.isStartElement()) {
                return true;
            }
            return false;
        }
    }
}

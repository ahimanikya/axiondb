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
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
import org.axiondb.io.AxionFileSystem;
import org.axiondb.types.CharacterType;
import org.axiondb.types.StringType;

/**
 *
 * @author karthikeyan s
 */
public class XMLTable extends BaseTable implements ExternalTable {
    
    /** The name of my ".data" file. */
    protected File _dataFile = null;
    protected URI  _uri = null;
    protected File _dbdir = null;
    
    protected boolean _readOnly = false;
    
    private File _dir;
    
    protected String _fileName;
    
    private String _rowName;
    
    private int _rowCount = -1;
    
    private int _currentRow = -1;
    
    private Properties prop = new Properties();
    
    public static final String PROP_FILENAME = "FILENAME";
    
    public static final String PROP_ROWNAME = "ROWNAME";
    
    public static final String PROP_READONLY = "TYPE";
    
    protected static final String META_FILE_EXT = ".META";
    
    protected static final String TYPE_FILE_EXT = ".TYPE";
    
    private String _readOnlyStatus;
    
    private XMLTableOrganizationContext context;
    
    private TransformerFactory tFactory;
    
    private Transformer transformer;
    
    private XMLStreamReader xmlStreamReader;
    
    private XMLInputFactory xmlInputFactory;
    
    private DocumentBuilderFactory factory;
    
    private Document document;
    
    private DocumentBuilder builder;
    
    private Element root;
    
    protected static AxionFileSystem FS = new AxionFileSystem();
    
    protected static final int CURRENT_META_VERSION = 3;
    
    private static final Set PROPERTY_KEYS = new HashSet(4);
    
    static {
        PROPERTY_KEYS.add(PROP_FILENAME);
        PROPERTY_KEYS.add(PROP_ROWNAME);
        PROPERTY_KEYS.add(PROP_READONLY);
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
        if(_readOnlyStatus.equalsIgnoreCase("READONLY")) {
            throw new AxionException("Operation not supported");
        }
        synchronized(this) {
            try {
                int rowid;
                for (IntIterator iter = rowids.iterator(); iter.hasNext();) {
                    rowid = iter.next();
                    root.removeChild(getRowNodeForId(rowid, root));
                    _rowCount--;
                }
                document.getDocumentElement().normalize();
                writeDomToFile();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    public void applyInserts(RowCollection rows) throws AxionException {
        if(_readOnlyStatus.equalsIgnoreCase("READONLY")) {
            throw new AxionException("Operation not supported");
        }
        synchronized(this) {
            try {
                int colCount = getColumnCount();
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row row = iter.next();
                    Node rowElem = document.createElement(_rowName);
                    root.appendChild(rowElem);
                    RowEvent event = new RowInsertedEvent(this, null, row);
                    for(int i = 0; i < colCount; i++) {
                        Node columnElem = document.createElement(getColumn(i).getName());
                        rowElem.appendChild(columnElem);
                        columnElem.appendChild(document.createTextNode(String.valueOf(row.get(i))));
                    }
                    _rowCount++;
                }
                document.getDocumentElement().normalize();
                writeDomToFile();
            } catch (Exception ex) {
                throw new AxionException(ex);
            }
        }
    }
    
    public void applyUpdates(RowCollection rows) throws AxionException {
        if(_readOnlyStatus.equalsIgnoreCase("READONLY")) {
            throw new AxionException("Operation not supported");
        }
        synchronized(this) {
            try {
                int colCount = getColumnCount();
                for (RowIterator iter = rows.rowIterator(); iter.hasNext();) {
                    Row newrow = iter.next();
                    Row oldrow = getRow(newrow.getIdentifier());
                    if (oldrow != null) {
                        RowEvent event = new RowUpdatedEvent(this, oldrow, newrow);
                    }
                    Element rowElem = (Element)getRowNodeForId(newrow.getIdentifier(), root);
                    for(int i = 0; i < colCount; i++) {
                        Node colNode = rowElem.getElementsByTagName(getColumn(i).getName()).item(0);
                        NodeList colChildren = colNode.getChildNodes();
                        for(int j = 0; j < colChildren.getLength(); j++) {
                            Node node = colChildren.item(j);
                            if(node.getNodeType() == Node.TEXT_NODE) {
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
    
    @Override
    public void drop() throws AxionException {
        super.drop();
        try {
            if(xmlStreamReader != null) {
                xmlStreamReader.close();
            }
        } catch (XMLStreamException ex) {
            throw new AxionException(ex);
        }
        if (!FileUtil.delete(getRootDir())) {
            throw new AxionException("Unable to delete \"" + getRootDir() + "\" during drop table " + getName());
        }
    }
    
    public int getNextRowId() {
        return _currentRow + 1;
    }
    
    public int getRowCount() {
        return _rowCount;
    }
    
    public void populateIndex(Index index) throws AxionException {
    }
    
    protected Row getRowByOffset(int idToAssign) throws AxionException {
        Row row;
        synchronized(this) {
            try {
                int colCount = getColumnCount();
                row = new SimpleRow(idToAssign, colCount);
                LinkedList<String> columns = getColumnValues(idToAssign);
                for(int i = 1; i <= colCount; i++) {
                    String columnValue = columns.get(i - 1);
                    row = trySettingColumn(idToAssign, row, i - 1, columnValue);
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
            for(int i = 1; i <= colCount; i++) {
                String columnValue = columns.get(i - 1);
                row = trySettingColumn(id, row, i - 1, columnValue);
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
    
    public boolean loadExternalTable(Properties prop) throws AxionException {
        try {
            if(context == null) {
                context = new XMLTableOrganizationContext();
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
        prop.put(PROP_LOADTYPE, "XML");
        if(_dataFile.exists())
        prop.put(PROP_FILENAME, _dataFile.getAbsolutePath());
        else
           prop.put(PROP_FILENAME,_uri.getPath());
        prop.put(PROP_ROWNAME, _rowName);
        prop.put(PROP_READONLY, _readOnlyStatus);
        return prop;
    }
    
    public void remount() throws AxionException {
    }
    
    protected boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }
    
    private String getDefaultDataFileExtension() {
        return "xml";
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
            _rowName = in.readUTF();
            _readOnlyStatus = in.readUTF();
            context = new XMLTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            if(getDataFile().getPath().startsWith("file:"));
            else if(getDataFile().exists());
            else{
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
                out.writeUTF(_rowName);
                out.writeUTF(_readOnlyStatus);
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
        return _dataFile;
    }
    private URI getURI() throws URISyntaxException{
    	
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
            ParserConfigurationException, SAXException, IOException {
        if(_readOnlyStatus.equalsIgnoreCase("READONLY")) {
            parseUsingStax();
            _rowCount = 0;
            _currentRow = 0;
            
            int event = -1;
            // get Row count.
            while(xmlStreamReader.hasNext()) {
                event = xmlStreamReader.next();
                if(event == XMLStreamConstants.START_ELEMENT &&
                        xmlStreamReader.getLocalName().equals(_rowName)) {
                    _rowCount++;
                }
            }
            parseUsingStax();
        } else {
            parseUsingDOM();
            _rowCount = 0;
            _currentRow = 0;
            if(root != null) {
                NodeList rows = root.getElementsByTagName(_rowName);
                for(int i = 0; i < rows.getLength(); i++) {
                    Node rowNode = rows.item(i);
                    if(rowNode.getNodeType() == Node.ELEMENT_NODE) {
                        _rowCount++;
                    }
                }
            }
        }
    }
    
    private void writeDomToFile() throws TransformerConfigurationException,
            TransformerException, ParserConfigurationException, SAXException, IOException, XMLStreamException {
        // Use a Transformer for output
        if(tFactory == null) {
            tFactory =
                    TransformerFactory.newInstance();
            transformer = tFactory.newTransformer();
        }
        transformer.setOutputProperty(OutputKeys.INDENT, "yes");
        DOMSource source = new DOMSource(document);
        try{
           StreamResult result = null; 
        if(getDataFile().exists()){
         result = new StreamResult(getDataFile());
        }else{ 
           result = new StreamResult(new File(getURI()));
        }
        transformer.transform(source, result);
        }catch(URISyntaxException e){
        
        }
    }
    
    private Node getRowNodeForId(int idToAssign, Element root) throws AxionException {        
        try {
            XPathFactory  factory = XPathFactory.newInstance();
            XPath xPath = factory.newXPath();        
            String xpathExpString = "//" + _rowName + "[" + String.valueOf(idToAssign) + "]";
            XPathExpression  xPathExpression = xPath.compile(xpathExpString);
            return (Node) xPathExpression.evaluate(root, XPathConstants.NODE);
        } catch (XPathExpressionException ex) {
            throw new AxionException(ex);
        }        
    }
    
    private LinkedList<String> getColumnValues(int rowId) throws AxionException {
        LinkedList<String> columns = new LinkedList<String>();
        if(_readOnlyStatus.equalsIgnoreCase("READWRITE")) {
            Element row = (Element)getRowNodeForId(rowId, root);
            for(int i = 0; i < getColumnCount(); i++) {
                Element column = (Element)row.getElementsByTagName(getColumn(i).getName()).item(0);
                if(column != null) {
                    NodeList colElems = column.getChildNodes();
                    for(int j = 0; j < colElems.getLength(); j++) {
                        Node nd = colElems.item(j);
                        if(nd.getNodeType() == Node.TEXT_NODE) {
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
       try{
        factory = DocumentBuilderFactory.newInstance();
        builder = factory.newDocumentBuilder();
           if(getDataFile().exists()){
        document = builder.parse(getDataFile());
          }
           else{
           document = builder.parse(getURI().toURL().openStream());
           }
        root = document.getDocumentElement();
       }
       catch(URISyntaxException e){
       }
    }
    
    private void parseUsingStax() throws FileNotFoundException, XMLStreamException {
        if(xmlStreamReader != null) {
            xmlStreamReader.close();
        }
        FileInputStream fileInputStream = new FileInputStream(getDataFile());
        if(xmlInputFactory == null) {
            xmlInputFactory = XMLInputFactory.newInstance();
            xmlInputFactory.setProperty(XMLInputFactory.IS_REPLACING_ENTITY_REFERENCES,Boolean.FALSE);
            xmlInputFactory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES,Boolean.FALSE);
            xmlInputFactory.setProperty(XMLInputFactory.IS_NAMESPACE_AWARE ,Boolean.FALSE);
            xmlInputFactory.setProperty(XMLInputFactory.IS_COALESCING ,Boolean.TRUE);
        }
        xmlStreamReader = xmlInputFactory.createFilteredReader(xmlInputFactory.
                createXMLStreamReader(fileInputStream), new XMLElementFilter());        
    }
    
    private LinkedList<String> readUsingStax(int rowId) throws AxionException {
        LinkedList<String> columns = new LinkedList<String>();
        int rowCount = _currentRow;
        int event = -1;
        
        try {
            if(_currentRow > rowId) {
                // read From First
                rowCount = 0;
                parseUsingStax();
            }
            
            while(xmlStreamReader.hasNext()) {
                
                event = xmlStreamReader.next();
                
                if(event == XMLStreamConstants.START_ELEMENT &&
                        xmlStreamReader.getLocalName().equals(_rowName)) {
                    rowCount++;
                }
                if(rowCount == rowId) {
                    break;
                }
            }
            for(int i = 0; i < getColumnCount(); i++) {
                // move to the column element.
                event = xmlStreamReader.next();
                while(!(event == XMLStreamConstants.START_ELEMENT &&
                        xmlStreamReader.getLocalName().equals(getColumn(i).getName()))) {
                    event = xmlStreamReader.next();
                }
                String elemText = xmlStreamReader.getElementText();
                elemText = (elemText == null)? "" : elemText;
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
            if(!_dataFile.exists()){
            try{
                _uri = new URI(_fileName);
            }catch(URISyntaxException e){
                
            }
            }
            _rowName = props.getProperty(PROP_ROWNAME);
            _readOnlyStatus = props.getProperty(PROP_READONLY);
            if (isNullString(_fileName)) {
                _fileName = getName() + "." + getDefaultDataFileExtension();
            }
        }
        
        public void updateProperties() {
            super.updateProperties();
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_XML);
            _props.setProperty(PROP_FILENAME, _fileName);
            _props.setProperty(PROP_ROWNAME, _rowName);
            _props.setProperty(PROP_READONLY, _readOnlyStatus);
        }
        
        public Set getRequiredPropertyKeys() {
            Set keys = new HashSet(PROPERTY_KEYS.size());
            keys.addAll(PROPERTY_KEYS);
            return keys;
        }
    }
    
    private static class XMLElementFilter implements StreamFilter {
        public boolean accept(XMLStreamReader reader) {
            if(reader.isStartElement()) {
                return true;
            }
            return false;
        }
    }
}

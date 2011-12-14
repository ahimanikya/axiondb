/*
 * WebTable.java
 *
 * Created on March 18, 2007, 1:38 PM
 *
 * To change this template, choose Tools | Template Manager
 * and open the template in the editor.
 */

package org.axiondb.engine.tables;

import java.io.EOFException;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import javax.swing.text.BadLocationException;
import javax.swing.text.EditorKit;
import javax.swing.text.Element;
import javax.swing.text.ElementIterator;
import javax.swing.text.html.HTMLDocument;
import javax.swing.text.html.HTMLEditorKit;
import org.apache.commons.collections.primitives.IntCollection;
import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.RowCollection;
import org.axiondb.io.FileUtil;

/**
 *
 * @author karthikeyan s
 */
public class WebTable extends DelimitedFlatfileTable {
    
    private static final Set PROPERTY_KEYS = new HashSet(5);
    
    /** Set of required keys for organization properties */
    private static final Set REQUIRED_KEYS = new HashSet(1);
    
    private static final String PROP_URL = "URL";
    
    private static final String PROP_REFRESH = "REFRESH";
    
    private static final String PROP_TABLE_NO = "TABLENUMBER";
    
    private int _tableNumber = -1;
    
    private boolean _refresh = false;
    
    private String url;
    
    private HTMLDocument doc;
    
    private URL web;
    
    private EditorKit kit;
    
    static {
        PROPERTY_KEYS.add(PROP_FIELDDELIMITER);
        PROPERTY_KEYS.add(PROP_QUALIFIER);
        PROPERTY_KEYS.add(PROP_URL);
        PROPERTY_KEYS.add(PROP_REFRESH);
        PROPERTY_KEYS.add(PROP_TABLE_NO);
    }
    
    /** Creates a new instance of WebTable */
    public WebTable(String name, Database db) throws AxionException {
        super(name, db, new WebTableLoader());
        setType(ExternalTable.WEB_TABLE_TYPE);
    }
    
    @Override
    public void applyDeletes(IntCollection rowIds) throws AxionException {
        throw new AxionException("Operation not supported");
    }
    
    @Override
    public void applyInserts(RowCollection rows) throws AxionException {
        throw new AxionException("Operation not supported");
    }
    
    @Override
    public void applyUpdates(RowCollection rows) throws AxionException {
        throw new AxionException("Operation not supported");
    }
    
    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        try {
            _lineSep = in.readUTF();
            _fieldSep = in.readUTF();
            _isFirstLineHeader = Boolean.valueOf(in.readUTF()).booleanValue();
            _fileName = in.readUTF();
            in.readUTF(); // _eol will be computed, keep for older version metadata
            _qualifier = in.readUTF();
            in.readUTF(); // _quoted will be computed, keep for older version metadata
            
            try {
                _rowCount = in.readInt();
            } catch (EOFException ignore) {
                // Goes here if metadata from an older version is parsed - ignore.
            }
            
            url = in.readUTF();
            _refresh = in.readBoolean();
            _tableNumber = in.readInt();
            
            context = new WebTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            createOrLoadDataFile();
            
        } catch (IOException ioex) {
            throw new AxionException("Unable to parse meta file for table " + getName(), ioex);
        }
    }
    
    protected void writeTableProperties(ObjectOutputStream out) throws AxionException {
        try {
            if (_lineSep != null && _fieldSep != null && _fileName != null) {
                out.writeUTF(_lineSep);
                out.writeUTF(_fieldSep);
                out.writeUTF(Boolean.toString(_isFirstLineHeader));
                out.writeUTF(_fileName);
                out.writeUTF(_lineSep);
                out.writeUTF(_qualifier);
                out.writeUTF("true");
                out.writeInt(_rowsToSkip);
                out.writeUTF(url);
                out.writeBoolean(_refresh);
                out.writeInt(_tableNumber);
            }
        } catch (IOException ioex) {
            throw new AxionException("Unable to write meta file for table " + getName(), ioex);
        }
    }
    
    public boolean loadExternalTable(Properties props) throws AxionException {
        context = new WebTableOrganizationContext();
        try {
            context.readOrSetDefaultProperties(props);
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
    
    private void createDataFile() throws AxionException {
        getURL();
        initializeEditorKit();
        javax.swing.text.Element element = getTableElement(_tableNumber);
        createCSV(element);
    }
    
    private void initializeEditorKit() {
        kit = new HTMLEditorKit();
        doc = (HTMLDocument)kit.createDefaultDocument();
        doc.putProperty("IgnoreCharsetDirective", Boolean.TRUE);
        try {
            kit.read(web.openStream(), doc, 0);
        } catch (IOException ex) {
            //ignore
        } catch (BadLocationException ex) {
            //ignore
        }
    }
    
    private void getURL() throws AxionException {
        try {
            if(url.startsWith("http") ||
                    url.startsWith("file")) {
                web =  new URI(url.trim()).toURL();
            } else {
                File file = new File(url.trim());
                if(file.exists()) {
                    web = file.toURI().toURL();
                }
            }
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }
    
    private javax.swing.text.Element getTableElement(int _tableNumber) {
        int count = 0;
        ElementIterator it = new ElementIterator(doc);
        javax.swing.text.Element element = null;
        while ((element = it.next()) != null ) {
            // read all table elements.
            if ("table".equalsIgnoreCase(element.getName())) {
                count++;
            }
            if(count == _tableNumber) {
                return element;
            }
        }
        return null;
    }
    
    private void createCSV(Element element) throws AxionException {
        File datafile = getDataFile();
        FileOutputStream out;
        try {
            out = new FileOutputStream(datafile);
        } catch (IOException ex) {
            throw new AxionException(ex);
        }
        
        ElementIterator eleIt = new ElementIterator(element);
        javax.swing.text.Element elem = null;
        LinkedList<javax.swing.text.Element> list =
                new LinkedList<javax.swing.text.Element>();
        javax.swing.text.Element th = null;        
        try {
            int i = 0;
            StringBuffer trData = new StringBuffer();
            while((elem = eleIt.next()) != null) {
                if(elem.getName().equalsIgnoreCase("tr")) {
                    if(i++ != 0) {
                        trData.append(_preferredLineSep);
                        out.write(trData.toString().getBytes());
                        trData = new StringBuffer();
                    }
                } else if(elem.getName().equalsIgnoreCase("th") ||
                        elem.getName().equalsIgnoreCase("td")) {
                    String dt = doc.getText(elem.getStartOffset(),
                            (elem.getEndOffset() - elem.getStartOffset())).trim();
                    if(trData.length() == 0) {
                        trData.append(_qualifier + dt + _qualifier);
                    } else {
                        trData.append(_fieldSep + _qualifier +
                                dt + _qualifier);
                    }
                }
            }
            trData.append(_preferredLineSep);
            out.write(trData.toString().getBytes());
            
            if(out != null) {
                out.flush();
                out.close();
            }
        } catch (Exception ex) {
            throw new AxionException(ex);
        }
    }
    
    private class WebTableOrganizationContext extends BaseFlatfileTableOrganizationContext {
        public Set getPropertyKeys() {
            Set baseKeys = super.getPropertyKeys();
            Set keys = new HashSet(baseKeys.size() + PROPERTY_KEYS.size());
            keys.addAll(baseKeys);
            keys.addAll(PROPERTY_KEYS);
            
            return keys;
        }
        
        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            props.put(PROP_LOADTYPE, ExternalTableFactory.TYPE_DELIMITED);
            super.readOrSetDefaultProperties(props);
            String rawFieldSep = props.getProperty(PROP_FIELDDELIMITER);
            if (isNullString(rawFieldSep)) {
                rawFieldSep = COMMA;
            }
            _fieldSep = fixEscapeSequence(rawFieldSep);
            
            // default line separator is new line
            String lineSep = System.getProperty("line.separator");
            if("".equals(_lineSep)) {
                _lineSep = fixEscapeSequence(lineSep);
            }
            
            // Support multiple record delimiter for delimited
            StringTokenizer tokenizer = new StringTokenizer(_lineSep, " ");
            ArrayList tmpList = new ArrayList();
            while(tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                tmpList.add(token);
                if(token.equals(lineSep)) {
                    _preferredLineSep = token;
                }
            }
            _lineSeps = (String[])tmpList.toArray(new String[0]);
            
            // determine the delimiter to be used for writing line
            if(_preferredLineSep == null || _preferredLineSep.length() == 0) {
                _preferredLineSep = _lineSeps[0];
            }
            
            _qualifier = fixEscapeSequence(props.getProperty(PROP_QUALIFIER));
            if (isNullString(_qualifier)) {
                _qualifier = EMPTY_STRING;
            } else {
                _qPattern = Pattern.compile(_qualifier);
            }
            
            try {
                _tableNumber = Integer.parseInt(props.getProperty(PROP_TABLE_NO).trim());
            } catch (NumberFormatException ex) {
                _tableNumber = 1;
            }
            url = props.getProperty(PROP_URL);
            _refresh = Boolean.valueOf(props.getProperty(PROP_REFRESH, "false").trim());
            props.put(PROP_FILENAME, getRootDir().getAbsoluteFile() + File.separator + "datafile.csv");
            _dataFile = new File(props.getProperty(PROP_FILENAME));
            if(_refresh) {
                if(_dataFile.exists()) {
                    _dataFile.delete();
                }
                try {
                    _dataFile.createNewFile();
                    createDataFile();
                } catch (IOException ex) {
                    throw new AxionException(ex);
                }
            } else {
                if(!_dataFile.exists()) {
                    try {
                        _dataFile.createNewFile();
                        createDataFile();
                    } catch (IOException ex) {
                        throw new AxionException(ex);
                    }
                }
            }
        }
        
        public void updateProperties() {
            super.updateProperties();
            
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_DELIMITED);
            _props.setProperty(PROP_FIELDDELIMITER, addEscapeSequence(_fieldSep));
            _props.setProperty(PROP_QUALIFIER, _qualifier);
            _props.setProperty(PROP_URL, url);
            _props.setProperty(PROP_TABLE_NO, String.valueOf(_tableNumber));
            _props.setProperty(PROP_REFRESH, String.valueOf(_refresh));
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

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
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.collections.primitives.ArrayIntList;
import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Row;
import org.axiondb.TableFactory;
import org.axiondb.TableOrganizationContext;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.io.FileUtil;
import org.axiondb.types.CharacterType;
import org.axiondb.types.LOBType;
import org.axiondb.types.ObjectType;
import org.axiondb.types.StringType;

/**
 * Base Flatfile Table<br>
 * 
 * TODO: Support for decimal and thousand separator, trailing/leading minus sign
 * TODO: Support for multiple record delimiter 
 * 
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 * @author Girish Patil
 */
public abstract class BaseFlatfileTable extends BaseDiskTable implements ExternalTable {

    protected static final int EOF = -1;
    protected static final char FILLER = ' ';

    public static final String PROP_FILENAME = "FILENAME";
    protected static final String PROP_ISFIRSTLINEHEADER = "ISFIRSTLINEHEADER";
    protected static final String PROP_RECORDDELIMITER = "RECORDDELIMITER";
    protected static final String PROP_ROWSTOSKIP = "ROWSTOSKIP";
    protected static final String PROP_MAXFAULTS = "MAXFAULTS";
    protected static final String PROP_TRIMWHITESPACE = "TRIMWHITESPACE";

    private static final Set PROPERTY_KEYS = new HashSet(4);

    static {
        PROPERTY_KEYS.add(PROP_FILENAME);
        PROPERTY_KEYS.add(PROP_CREATE_IF_NOT_EXIST);
        PROPERTY_KEYS.add(PROP_ISFIRSTLINEHEADER);
        PROPERTY_KEYS.add(PROP_RECORDDELIMITER);
        PROPERTY_KEYS.add(PROP_ROWSTOSKIP);
        PROPERTY_KEYS.add(PROP_MAXFAULTS);
        PROPERTY_KEYS.add(PROP_TRIMWHITESPACE);
        PROPERTY_KEYS.add(ExternalTable.PROP_LOADTYPE);
    }

    public static String PARAM_KEY_FILE_NAME = "FLAT_FILE_NAME";
    public static String PARAM_KEY_TEMP_FILE_NAME = "TEMP_FLAT_FILE_NAME";
    public static String PARAM_KEY_FILE_DIR = "FLAT_FILE_DIR";

    public BaseFlatfileTable(String name, Database db, TableFactory factory) throws AxionException {
        super(name, db, factory);
    }

    public void addColumn(Column col, boolean metaUpdateNeeded) throws AxionException {
        if (col.getDataType() instanceof LOBType || col.getDataType() instanceof ObjectType) {
            throw new UnsupportedOperationException("Lob or Object Type not supported for this Table Type");
        }
        super.addColumn(col, metaUpdateNeeded);
    }

    /**
     * Loads external data using the given properties table - should be called only once
     * by the table factory.
     * 
     * @param table Table to be set
     * @param props Properties for Table
     * @exception AxionException thrown while setting Properties
     */
    public boolean loadExternalTable(Properties props) throws AxionException {
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

    public void remount() throws AxionException {
        remount(getRootDir(), false);
    }

    public static String addEscapeSequence(String srcString) {
        StringBuffer buf = new StringBuffer(srcString.length() + 2);

        char[] srcArray = srcString.toCharArray();
        for (int i = 0; i < srcArray.length; i++) {
            char c = srcArray[i];
            switch (c) {
                case '\t':
                    buf.append("\\t");
                    break;

                case '\r':
                    buf.append("\\r");
                    break;

                case '\b':
                    buf.append("\\b");
                    break;

                case '\n':
                    buf.append("\\n");
                    break;

                case '\f':
                    buf.append("\\f");
                    break;

                default:
                    buf.append(c);
                    break;
            }
        }

        return buf.toString();
    }

    protected void createOrLoadDataFile() throws AxionException {
    	createOrLoadDataFile(_isCreateDataFileIfNotExist); 
    }
    
    protected void createOrLoadDataFile(boolean createNewDataFile) throws AxionException {
        if(!createNewDataFile && !getDataFile().exists()) {
            throw new AxionException("Data file \"" + getDataFile() + "\" for table " + getName() + " not found.");
        }
        
        try {
            if (getDataFile().createNewFile()) {
                writeHeader(getOutputStream());
            } else {
                getOutputStream();
            }
            getInputStream();
        } catch (IOException ioex) {
            throw new AxionException("Unable to create data file \"" + getDataFile() + "\" for table " + getName() + ".", ioex);
        }
    }

    public static String fixEscapeSequence(String srcString) {
        // no escape sequences, return string as is
        if (srcString == null || srcString.length() < 2)
            return srcString;

        char[] srcArray = srcString.toCharArray();
        char[] tgtArray = (char[]) srcArray.clone();
        Arrays.fill(tgtArray, ' ');
        int j = 0;
        // e.g. curString = "\\r\\n" = {'\\', 'r', '\\', 'n'}
        for (int i = 0; i < srcArray.length; i++) {
            char c = srcArray[i];
            if ((i + 1) == srcArray.length) {
                // this is last char, so put it as is
                tgtArray[j++] = c;
                break;
            }

            if (c == '\\') {
                switch (srcArray[i + 1]) {
                    case 't':
                        c = '\t';
                        break;
                    case 'r':
                        c = '\r';
                        break;
                    case 'b':
                        c = '\b';
                        break;
                    case 'n':
                        c = '\n';
                        break;
                    case 'f':
                        c = '\f';
                        break;
                }
                i++;
            }
            tgtArray[j++] = c;
        }

        return new String(tgtArray, 0, j);
    }

    protected File getDataFile() {
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

        super.renameTableFiles(oldName, name);

        if (newFileProps != null){
            newDir = newFileProps.getProperty(PARAM_KEY_FILE_DIR);
            newFileName = newFileProps.getProperty(PARAM_KEY_FILE_NAME);
            oldFileName = newFileProps.getProperty(PARAM_KEY_TEMP_FILE_NAME);
        }

        if (newDir != null){
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

    protected File getLobDir() {
        throw new UnsupportedOperationException("Lobs Datatype is not supported");
    }

    protected long ignoreRowsToSkip() throws AxionException {
        if (_rowsToSkip > 0) {
            int offset = 0;
            int i = _rowsToSkip;

            while (i-- > 0) {
                offset += nextLineLength(offset);
            }

            return offset;
        } else if (_isFirstLineHeader) {
            // Handle old versions of flatfiles where RowsToSkip is undefined and first
            // line header is true.
            return nextLineLength(0);
        }
        return 0;
    }

    protected void initializeTable() throws AxionException {
        try {
            int faultCount = 0;
            _rowCount = 0;

            final long fileLength = FileUtil.getLength(getDataFile());

            long fileOffset = ignoreRowsToSkip();
            while (true) {
                // If at EOF or current offset + length of line separator string is larger
                // than length of file (less EOF character), then terminate.
                // XXX: In case _lineSep holds multiple record delimiter we may have issue here ?
                if (-1 == fileOffset || (fileOffset + _lineSep.length() >= fileLength - 1)) {
                    break;
                }

                getPidxList().add(fileOffset);
                int idToAssign = getPidxList().size() - 1;

                try {
                    _rowCount++;
                    getRowByOffset(idToAssign, fileOffset);
                } catch (AxionException e) {
                    // ignore this line and read next line
                    // set row as invalid so that we will not attempt to read
                    // next time
                    getPidxList().set(idToAssign, INVALID_OFFSET);
                    _freeIds.add(idToAssign);
                    _rowCount--;

                    if (++faultCount > _maxFaults) {
                        // TODO: Write bad rows to a file with .bad extension
                        String msg = "Fault tolerance threshold (" + _maxFaults + ") exceeded for table " + getName() + ". ";
                        throw new AxionException(msg + e.getMessage(), e);
                    }
                }

                fileOffset = getInputStream().getPos();
            }
            getPidxList().flush();
        } catch (Exception e) {
            throw new AxionException(e);
        }
    }

    abstract protected boolean isEndOfRecord(int recLength, int nextChar, BufferedDataInputStream data) throws IOException;

    protected boolean isEOF(int nextChar) {
        return nextChar == EOF;
    }

    protected boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }

    protected String getDefaultDataFileExtension() {
        return "txt";
    }

    protected void reloadFilesAfterTruncate() throws AxionException {
        // zero out freeIds file
        _freeIds = new ArrayIntList();
        writeFridFile();

        // Create zero-record data file (with header if required).
        createOrLoadDataFile(true);
        initFiles(getDataFile(), true);

        initializeTable();
        initializeRowCount();
    }

    protected Row trySettingColumn(Row row, int i, Object colValue) throws AxionException {
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

    protected Object evaluateForNull(Object colValue,  DataType datatype) {
    	if(null == colValue ){
    		 return null;
    	} else if (datatype instanceof CharacterType) {
            int colWidth = datatype.getPrecision();
            String str = colValue.toString();
            return (colWidth <= 0 || (str.length() == colWidth && str.trim().length() == 0)) ? null : colValue;
        } else if (!(datatype instanceof StringType) && !(colValue instanceof Number) && (colValue.toString()).trim().length() == 0) {
            return null;
        }

        return colValue;
    }

    protected abstract void writeHeader(BufferedDataOutputStream data2) throws AxionException;

    protected int nextLineLength(long fileOffset) throws AxionException {
        if (fileOffset < 0) {
            return EOF;
        }

        try {
            int nextChar;
            int recLength = 0;
            BufferedDataInputStream data = getInputStream();

            data.seek(fileOffset);
            while (true) {
                nextChar = data.read();
                recLength++;

                if (isEOF(nextChar)) {
                    if (recLength > 1) {
                        //-- EOF reached but has some data
                        return recLength;
                    }
                    return EOF;
                }

                if (isEndOfRecord(recLength, nextChar, data)) {
                    //recLength += (_lineSep.length() - 1);
                    recLength = (int) (data.getPos() - fileOffset) ;
                    break;
                }
            }
            return recLength;

        } catch (Exception e) {
            throw new AxionException("Unable to parse data file: ", e);
        }
    }

    protected abstract class BaseFlatfileTableOrganizationContext extends BaseTableOrganizationContext {
        public Set getPropertyKeys() {
            Set keys = new HashSet(PROPERTY_KEYS.size());
            keys.addAll(PROPERTY_KEYS);
            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            assertValidPropertyKeys(props);

            String rawLineSep = props.getProperty(PROP_RECORDDELIMITER);
            if (rawLineSep != null) {
                _lineSep = fixEscapeSequence(rawLineSep);
            } else {
                _lineSep = "";
            }

            String firstLineHeader = props.getProperty(PROP_ISFIRSTLINEHEADER, "false");
            _isFirstLineHeader = Boolean.valueOf(firstLineHeader).booleanValue();

            _fileName = props.getProperty(PROP_FILENAME);
            if (isNullString(_fileName)) {
                _fileName = getName() + "." + getDefaultDataFileExtension();
            }
            
            if ((props.getProperty(PROP_CREATE_IF_NOT_EXIST) != null) 
                 && ("FALSE".equalsIgnoreCase(props.getProperty(PROP_CREATE_IF_NOT_EXIST)))) {
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
                _maxFaults = Long.parseLong(maxFaultStr);
            } catch (NumberFormatException e) {
                _maxFaults = Long.MAX_VALUE;
            }

            // Negative values are meaningless...in this case use Long.MAX_VALUE to
            // represent unlimited fault threshold.
            if (_maxFaults < 0L) {
                _maxFaults = Long.MAX_VALUE;
            }

            String trimWhiteSpaceStr = props.getProperty(PROP_TRIMWHITESPACE,"false");
            _trimWhiteSpace = Boolean.valueOf(trimWhiteSpaceStr).booleanValue();
        }

        public void updateProperties() {
            super.updateProperties();

            setProperty(PROP_RECORDDELIMITER, addEscapeSequence(_lineSep));
            setProperty(PROP_ISFIRSTLINEHEADER, Boolean.toString(_isFirstLineHeader));
            setProperty(PROP_FILENAME, _fileName);
            setProperty(PROP_ROWSTOSKIP, Integer.toString(_rowsToSkip));
            setProperty(PROP_MAXFAULTS, Long.toString(_maxFaults));
            setProperty(PROP_CREATE_IF_NOT_EXIST, Boolean.toString(_isCreateDataFileIfNotExist));
	    setProperty(PROP_TRIMWHITESPACE,Boolean.toString(_trimWhiteSpace));
        }

        public Set getRequiredPropertyKeys() {
            return getBaseRequiredPropertyKeys();
        }
    }

    protected TableOrganizationContext context;
    protected String _fileName;
    protected boolean _isFirstLineHeader = false;
    protected int _rowsToSkip = 0;
    protected long _maxFaults = Long.MAX_VALUE;
    protected boolean _isCreateDataFileIfNotExist = true;
    protected boolean _trimWhiteSpace = false;

    protected String _lineSep;
}

/*
 *
 * =======================================================================
 * Copyright (c) 2002-2007 Axion Development Team.  All rights reserved.
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

import java.io.CharArrayWriter;
import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.ExternalTableLoader;
import org.axiondb.Row;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.io.FileUtil;
import org.apache.commons.collections.primitives.ArrayUnsignedIntList;
import org.axiondb.io.AxionFileSystem.PidxList;
import org.axiondb.io.CharStreamTokenizer;


/**
 * A disk-resident Delimited Flatfile {@link org.axiondb.Table}.<br>
 *
 * TODO: Support for multiple delimiter for field and record
 * TODO: Support for treating consecutive delimiter as one
 *
 * @version
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
@SuppressWarnings(value = "unchecked")
public class DelimitedFlatfileTable extends BaseFlatfileTable {

    protected static final String EMPTY_STRING = "";
    protected static final String COMMA = ",";
    public static final String PROP_FIELDDELIMITER = "FIELDDELIMITER"; // NOI18N
    public static final String PROP_QUALIFIER = "QUALIFIER"; // NOI18N
    public static final String PROP_VALIDATION = "VALIDATION"; // NOI18N
    byte[] QUALIFIER_BYTES;
    byte[] EMPTY_STRING_BYTES = EMPTY_STRING.getBytes();
    byte[] LINESEP_BYTES;
    byte[] FIELDSEP_BYTES;
    private static final Set PROPERTY_KEYS = new HashSet(2);
    /** Set of required keys for organization properties */
    private static final Set REQUIRED_KEYS = new HashSet(1);
    static {
        PROPERTY_KEYS.add(PROP_VALIDATION);
        PROPERTY_KEYS.add(PROP_FIELDDELIMITER);
        PROPERTY_KEYS.add(PROP_QUALIFIER);
    }

    public DelimitedFlatfileTable(String name, Database db) throws AxionException {
        super(name, db, new DelimitedFlatfileTableLoader());
        setType(ExternalTable.DELIMITED_TABLE_TYPE);
    }

    public DelimitedFlatfileTable(String name, Database db, ExternalTableLoader loader) throws AxionException {
        super(name, db, loader);
    }

    @Override
    protected String getDefaultDataFileExtension() {
        return "csv";
    }

    protected Row getRowByOffset(int idToAssign, long ptr) throws AxionException {
        Row row = null;
        try {
            if (null == _readStream) {
                _readStream = getInputStream();
            }
            synchronized (_readStream) {
                row = _streamTokenizer.readAndSplitLine(_readStream, ptr, _colCount, _trimWhiteSpace, _dataTypes);
            }
            row.setIdentifier(idToAssign);
        } catch (IOException e) {
            throw new AxionException(e, 22031);
        }
        return row;
    }

    protected boolean isEndOfRecord(int recLength, int nextChar, BufferedDataInputStream data) throws IOException {
        return _streamTokenizer.isEndOfRecord(nextChar, data);
    }

    @Override
    public boolean loadExternalTable(Properties props) throws AxionException {
        context = new DelimitedTableOrganizationContext();
        return super.loadExternalTable(props);
    }

    @Override
    protected void initializeTable() throws AxionException {
        try {
            int faultCount = 0;
            final long endOffset = FileUtil.getLength(getDataFile());
            long fileOffset = ignoreRowsToSkip();
            PidxList pidx = getPidxList();
            //while (fileOffset < endOffset) { // This condition will always be true, hence while creating table it goes into infite loop.
            while (true) {
                // If at EOF or current offset + length of line separator string is larger
                // than length of file (less EOF character), then terminate.
                // XXX: In case _lineSep holds multiple record delimiter we may have issue here ?
                if (-1 == fileOffset || (fileOffset + _lineSep.length() >= endOffset - 1)) {
                    break;
                }
                pidx.add(fileOffset);
                if (_validate) {
                    try {
                        Row row = _streamTokenizer.readAndSplitLine(_readStream, fileOffset, _colCount, _trimWhiteSpace, _dataTypes);
                        for (int i = 0; i < _colCount; i++) {
                            row = trySettingColumn(row, i, row.get(i));
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
                } else {
                    _streamTokenizer.skipLine(_readStream);
                    _rowCount++;
                }
                fileOffset = _readStream.getPos();
            }
            getPidxList().flush();
        } catch (Exception e) {
            throw new AxionException(e);
        }
    }
    
    public Properties getTableProperties() {
        return context.getTableProperties();
    }

    @Override
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
            //_validate = in.readBoolean(); //12-Dec-2007: Boolean value Not found in Meta File
            
            context = new DelimitedTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            createOrLoadDataFile();
        } catch (IOException ioex) {
            throw new AxionException("Unable to parse meta file for table " + getName(), ioex);
        }
    }

    protected void writeHeader(BufferedDataOutputStream dataFile) throws AxionException {
        if (_isFirstLineHeader) {
            try {
                CharArrayWriter header = new CharArrayWriter();
                for (int i = 0; i < _colCount; i++) {
                    if (i != 0) {
                        header.write(_fieldSep);
                    }
                    header.write(getColumn(i).getName());
                }
                header.write(_preferredLineSep);
                dataFile.write(header.toString().getBytes());
                header.close();
            } catch (IOException ioex) {
                throw new AxionException("Unable to write header for table: " + getName(), ioex);
            }
        }
    }

    protected void writeRow(BufferedDataOutputStream buffer, Row row) throws AxionException {
        Object colValue = null;
        try {
            for (int i = 0; i < _colCount; i++) {
                colValue = row.get(i);

                if (i != 0) {
                    buffer.write(FIELDSEP_BYTES);
                }
                byte[] qualifier = isEscapeRequired(_dataTypes[i]) ? QUALIFIER_BYTES : EMPTY_STRING_BYTES;
                if (colValue != null) {
                    buffer.write(qualifier);
                    String val = _dataTypes[i].toString(colValue);
                    if (_isQuoted && val.indexOf(_qualifier) != -1) {
                        // escape the quealifier in the data string.
                        val = _qPattern.matcher(val).replaceAll(_qualifier + _qualifier);
                    }
                    buffer.write(val.getBytes());
                    buffer.write(qualifier);
                } else {
                    buffer.write(EMPTY_STRING_BYTES); // Write Null column
                }
            }
            // write new line
            buffer.write(LINESEP_BYTES);
        } catch (IOException e) {
            throw new AxionException("Error writing row: " + row, e);
        }
    }

    @Override
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
                out.writeBoolean(_validate);
            }
        } catch (IOException ioex) {
            throw new AxionException("Unable to write meta file for table " + getName(), ioex);
        }
    }

    private boolean isEscapeRequired(DataType type) {
        switch (type.getJdbcType()) {
            case Types.CHAR:
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
            case Types.VARCHAR:
                return true;
            default:
                return false;
        }
    }

    private class DelimitedTableOrganizationContext extends BaseFlatfileTableOrganizationContext {

        @Override
        public Set getPropertyKeys() {
            Set baseKeys = super.getPropertyKeys();
            Set keys = new HashSet(baseKeys.size() + PROPERTY_KEYS.size());
            keys.addAll(baseKeys);
            keys.addAll(PROPERTY_KEYS);

            return keys;
        }

        @Override
        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            super.readOrSetDefaultProperties(props);

            String rawFieldSep = props.getProperty(PROP_FIELDDELIMITER);
            if (rawFieldSep == null || rawFieldSep.length() == 0) {
                rawFieldSep = COMMA;
            }
            _fieldSep = fixEscapeSequence(rawFieldSep);
            _fieldSepChar = _fieldSep.toCharArray();

            // default line separator is new line
            String lineSep = System.getProperty("line.separator");
            if ("".equals(_lineSep)) {
                _lineSep = fixEscapeSequence(lineSep);
            }

            String validation = props.getProperty(PROP_VALIDATION);
            if (!isNullString(validation)) {
                if ("false".equalsIgnoreCase(validation)) {
                    _validate = false;
                }
            }

            // Support multiple record delimiter for delimited
            StringTokenizer tokenizer = new StringTokenizer(_lineSep, " ");
            ArrayList tmpList = new ArrayList();
            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();
                tmpList.add(token);
                if (token.equals(lineSep)) {
                    _preferredLineSep = token;
                }
            }

            _lineSeps = (String[]) tmpList.toArray(new String[0]);
            _lineSepsChar = new char[tmpList.size()][];
            for (int i = 0, I = tmpList.size(); i < I; i++) {
                _lineSepsChar[i] = ((String) tmpList.get(i)).toCharArray();
            }

            // determine the delimiter to be used for writing line
            if (_preferredLineSep == null || _preferredLineSep.length() == 0) {
                _preferredLineSep = _lineSeps[0];
            }

            _qualifier = fixEscapeSequence(props.getProperty(PROP_QUALIFIER));
            if (isNullString(_qualifier)) {
                _qualifier = EMPTY_STRING;
                _isQuoted = false;
                _qualifierChar = new char[0];
            } else {
                _qPattern = Pattern.compile(_qualifier);
                _isQuoted = true;
                _qualifierChar = _qualifier.toCharArray();
            }

            _colCount = getColumnCount();
            _dataTypes = getDataTypes();

            QUALIFIER_BYTES = _qualifier.getBytes();
            LINESEP_BYTES = _preferredLineSep.getBytes();
            FIELDSEP_BYTES = _fieldSep.getBytes();

            _streamTokenizer = new CharStreamTokenizer(_fieldSepChar, _lineSepsChar, _qualifierChar, _isQuoted);
        }

        @Override
        public void updateProperties() {
            super.updateProperties();

            _props.setProperty(PROP_VALIDATION, String.valueOf(_validate));
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_DELIMITED);
            _props.setProperty(PROP_FIELDDELIMITER, addEscapeSequence(_fieldSep));
            _props.setProperty(PROP_QUALIFIER, _qualifier);
        }

        @Override
        public Set getRequiredPropertyKeys() {
            Set baseRequiredKeys = getBaseRequiredPropertyKeys();
            Set keys = new HashSet(baseRequiredKeys.size() + REQUIRED_KEYS.size());
            keys.addAll(baseRequiredKeys);
            keys.addAll(REQUIRED_KEYS);

            return keys;
        }
    }
    protected String _fieldSep;
    private char[] _fieldSepChar;
    protected String _preferredLineSep;
    protected String[] _lineSeps;
    protected char[][] _lineSepsChar;
    protected String _qualifier;
    private char[] _qualifierChar;
    protected boolean _isQuoted;
    protected Pattern _qPattern;
    private boolean _validate = true;
    private DataType[] _dataTypes;
    private int _colCount;
    private CharStreamTokenizer _streamTokenizer;
}
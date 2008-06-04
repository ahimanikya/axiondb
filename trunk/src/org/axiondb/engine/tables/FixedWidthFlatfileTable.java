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

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.axiondb.AxionException;
import org.axiondb.Column;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Row;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;

/**
 * A disk-resident Fixed Width Flatfile {@link org.axiondb.Table}.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 * @author Jonathan Giron
 */
public final class FixedWidthFlatfileTable extends BaseFlatfileTable {

    // TODO: Add Record Trailer byte size

    public static final String PROP_HEADERBYTESOFFSET = "HEADERBYTESOFFSET";

    private static final Set PROPERTY_KEYS = new HashSet(2);

    static {
        PROPERTY_KEYS.add(PROP_HEADERBYTESOFFSET);
    }

    public FixedWidthFlatfileTable(String name, Database db) throws AxionException {
        super(name, db, new FixedWidthFlatfileTableLoader());
        setType(ExternalTable.FW_TABLE_TYPE);
    }

    public void addColumn(Column col, boolean metaUpdateNeeded) throws AxionException {
        super.addColumn(col, metaUpdateNeeded);
        updateRecordLength();
    }

    protected Row getRowByOffset(int idToAssign, long ptr) throws AxionException {
        BufferedDataInputStream data = getInputStream();
        int colCount = getColumnCount();
        Row row = new SimpleRow(idToAssign, colCount);

        try {
            synchronized (data) {
                int colLength = 0;
                int linePtr = 0;
                char[] charArray = readLine(data, ptr);

                for (int i = 0; i < colCount; i++) {
                    colLength = getColumnSize(i);
                    String columnValue = new String(charArray, linePtr, colLength);
                    if( _trimWhiteSpace) {
                        columnValue = columnValue.trim();
                        if (columnValue.length() == 0) {
                        	columnValue = null; 
                        }
                    }
                    row = trySettingColumn(row, i, columnValue);
                    linePtr += colLength;
                }
            }
            return row;
        } catch (Exception e) {
            if (e instanceof AxionException) {
                throw (AxionException) e;
            }
            throw new AxionException(e);
        }
    }

    protected int nextLineLength(final long fileOffset) throws AxionException {
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
                    recLength = (recLength == 1 ? recLength : _recordLength);
                    break;
                }
            }
            return recLength;

        } catch (Exception e) {
            throw new AxionException("Unable to parse data file: ", e);
        }
    }

    protected long ignoreRowsToSkip() throws AxionException {
        long offset = super.ignoreRowsToSkip();
        if (offset > 0) {
            return offset;
        } else if (_headerBytesOffset > 0) {
            return _headerBytesOffset;
        }
        return 0;
    }

    protected void initializeTable() throws AxionException {
        _lineCharArray = new char[_recordLength];
        super.initializeTable();
    }

    protected boolean isEndOfRecord(int recLength, int nextChar, BufferedDataInputStream data) throws IOException {
        if (isEOF(nextChar)) {
            return true;
        }
    	
    	if (recLength >= _recordLength){
    		if (!("".equals(_lineSep)) && _lineSep.charAt(0) != nextChar) {
    			throw new IOException("Corrupted data, record delimeter not found at specified record length.");
    		} else {
                return true;
            }
    	}

        if (!("".equals(_lineSep)) && _lineSep.charAt(0) == nextChar) {
            char[] charBuf = _lineSep.toCharArray();
            // Look ahead to see whether the following chars match EOL.
            long lastDataFileOffset = data.getPos();
            for (int i = 1, I =_lineSep.length(); i < I; i++) {
                if (charBuf[i] != (char) data.read()) {
                    data.seek(lastDataFileOffset);
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public boolean loadExternalTable(Properties props) throws AxionException {
        context = new FixedwidthTableOrganizationContext();
        return super.loadExternalTable(props);
    }

    public Properties getTableProperties() {
        return context.getTableProperties();
    }

    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        try {
            _lineSep = in.readUTF();
            _isFirstLineHeader = Boolean.valueOf(in.readUTF()).booleanValue();
            in.readUTF(); // _eol will be computed , keep this for older version metadata
            _headerBytesOffset = in.readInt();
            _recordLength = in.readInt();
            _fileName = in.readUTF();

            context = new FixedwidthTableOrganizationContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            createOrLoadDataFile();
            _lineCharArray = new char[_recordLength];
        } catch (IOException e) {
            throw new AxionException("Unable to parse meta file for table " + getName(), e);
        }
    }

    protected synchronized void renameTableFiles(String oldName, String name) {
        super.renameTableFiles(oldName, name);
        updateRecordLength();
    }

    private void updateRecordLength() {
        if (context != null) {
            _recordLength = 0;
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                _recordLength += getColumnSize(i);
            }
            _lineCharArray = new char[_recordLength];
        }
    }

    protected void writeHeader(BufferedDataOutputStream dataFile) throws AxionException {
        if (_isFirstLineHeader) {
            try {
                for (int i = 0, I = getColumnCount(); i < I; i++) {
                    dataFile.write(writeColumn(i, getColumn(i).getName()));
                }
                dataFile.write(_lineSep.getBytes());
            } catch (IOException ioex) {
                throw new AxionException("Unable to write header for table: " + getName(), ioex);
            }
        }
    }

    protected void writeRow(BufferedDataOutputStream out, Row row) throws AxionException {

        Object colValue = null;
        DataType type = null;

        try {
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                colValue = row.get(i);
                type = getColumn(i).getDataType();
                out.write(writeColumn(i, type.toString(colValue)));
            }

            // write new line
            out.write(_lineSep.getBytes());
        } catch (IOException e) {
            throw new AxionException("Error writing row: " + row, e);
        }
    }

    protected void writeTableProperties(ObjectOutputStream out) throws AxionException {
        try {
            if (_lineSep != null && _fileName != null) {
                out.writeUTF(_lineSep);
                out.writeUTF(Boolean.toString(_isFirstLineHeader));
                out.writeUTF(_lineSep);
                out.writeInt(_headerBytesOffset);
                out.writeInt(_recordLength);
                out.writeUTF(_fileName);
            }
        } catch (IOException e) {
            throw new AxionException("Unable to write meta file for table " + getName(), e);
        }
    }

    private int getColumnSize(int index) {
        return getColumn(index).getDataType().getColumnDisplaySize();
    }

    private char[] readLine(BufferedDataInputStream data, long fileOffset) throws AxionException {
        Arrays.fill(_lineCharArray, FILLER);
        int recLength = 0;
        try {
            int nextChar;
            data.seek(fileOffset);

            while (true) {
                nextChar = data.read();
                if (isEndOfRecord(recLength, nextChar, data)) {
                    if (recLength == 0) {
                        throw new AxionException("Empty line detected - invalid.");
                    } else if (_lineSep.length() == 0 && !isEOF(nextChar)) {
                        // If this is a packed data file, move pointer back by one - since
                        // there isn't a record delimiter, our call to data.read() has set
                        // the file pointer to the first char of the next record.
                        data.seek(data.getPos() - 1);
                    }
                    break;
                }
                _lineCharArray[recLength++] = ((char) nextChar);
            }

            return _lineCharArray;

        } catch (IOException ioex) {
            throw new AxionException("Unable to parse data file: ", ioex);
        }
    }

    private byte[] writeColumn(int colIndex, String value) {
        if (value == null) {
            value = " ";
        }

        byte byteData[] = new byte[getColumnSize(colIndex)];
        Arrays.fill(byteData, (byte) FILLER);
        byte colValue[] = value.getBytes();

        // truncate if required
        int len = colValue.length <= byteData.length ? colValue.length : byteData.length;
        System.arraycopy(colValue, 0, byteData, 0, len);
        return byteData;
    }

    private class FixedwidthTableOrganizationContext extends BaseFlatfileTableOrganizationContext {
        public Set getPropertyKeys() {
            Set baseKeys = super.getPropertyKeys();
            Set keys = new HashSet(baseKeys.size() + PROPERTY_KEYS.size());
            keys.addAll(baseKeys);
            keys.addAll(PROPERTY_KEYS);

            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            super.readOrSetDefaultProperties(props);

            // compute record length; if not set yet
            for (int i = 0, I = getColumnCount(); i < I; i++) {
                _recordLength += getColumnSize(i);
            }
            _recordLength += _lineSep.length();

            try {
                _headerBytesOffset = Integer.parseInt(props.getProperty(PROP_HEADERBYTESOFFSET));
            } catch (NumberFormatException e) {
                // optional argument, ignore if not set
            }

            // IsFirstLineHeader will override HeaderBytesOffset.
            _isFirstLineHeader = Boolean.valueOf(props.getProperty(PROP_ISFIRSTLINEHEADER.toUpperCase(), "false")).booleanValue();
        }

        public void updateProperties() {
            super.updateProperties();
            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_FIXEDWIDTH);
            _props.setProperty(PROP_HEADERBYTESOFFSET, Integer.toString(_headerBytesOffset));
        }
    }

    private int _headerBytesOffset = 0;
    private char[] _lineCharArray;
    private int _recordLength;

}

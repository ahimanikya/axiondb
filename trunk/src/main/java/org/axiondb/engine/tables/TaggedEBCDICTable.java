/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2004 Axion Development Team.  All rights reserved.
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
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.ExternalTable;
import org.axiondb.Row;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.io.BufferedDataInputStream;
import org.axiondb.io.BufferedDataOutputStream;
import org.axiondb.util.AsciiEbcdicEncoder;

/**
 * A disk-resident Fixed Width Flatfile {@link org.axiondb.Table}.
 * <p>
 * Example: create external table test1( col1 datatype, col2 datatype, ...)
 * organization(loadtype='taggedebcdic' RecordLength='213', HeaderBytesOffset='24',
 * tagLength='4', minTagCount='1', maxTagCount='48', tagByteCount='0',
 * recordTrailerByteCount='54' FileName='C:/hawaii/test/input_data.txt',
 * TagByteCount='2', en='cp037')
 * 
 * @version  
 * @author Sudhi Seshachala
 * @author Jonathan Giron
 * @author Ahimanikya Satapathy
 */

public class TaggedEBCDICTable extends BaseFlatfileTable {

    public static final String PROP_HEADERBYTESOFFSET = "HEADERBYTESOFFSET";
    public static final String PROP_RECORDLENGTH = "RECORDLENGTH";
    public static final String PROP_TAGLENGTH = "TAGLENGTH";
    public static final String PROP_MINTAGCOUNT = "MINTAGCOUNT";
    public static final String PROP_MAXTAGCOUNT = "MAXTAGCOUNT";
    public static final String PROP_RECORDTRAILERBYTECOUNT = "RECORDTRAILERBYTECOUNT";
    public static final String PROP_TAGBYTECOUNT = "TAGBYTECOUNT";
    public static final String PROP_ENCODING = "EN";
    private static final char EBCDIC_FILLER = '@';
    private static final char CR = '\r';
    private static final char NL = '\n';

    private static final Set PROPERTY_KEYS = new HashSet(8);
    private static final Set REQUIRED_KEYS = new HashSet(8);

    static {
        PROPERTY_KEYS.add(PROP_HEADERBYTESOFFSET);
        PROPERTY_KEYS.add(PROP_RECORDLENGTH);
        PROPERTY_KEYS.add(PROP_TAGLENGTH);
        PROPERTY_KEYS.add(PROP_MINTAGCOUNT);
        PROPERTY_KEYS.add(PROP_MAXTAGCOUNT);
        PROPERTY_KEYS.add(PROP_RECORDTRAILERBYTECOUNT);
        PROPERTY_KEYS.add(PROP_TAGBYTECOUNT);
        PROPERTY_KEYS.add(PROP_ENCODING);
    }

    public TaggedEBCDICTable(String name, Database db) throws AxionException {
        super(name, db, new TaggedEBCDICTableLoader());
        setType(ExternalTable.TAGGED_EBCDIC_TABLE_TYPE);
    }

    protected Row getRowByOffset(int idToAssign, long ptr) throws AxionException {
        try {
            BufferedDataInputStream data = getInputStream();
            int colCount = getColumnCount();
            Row row = new SimpleRow(idToAssign, colCount);
            _tagID = _minTagCount;

            synchronized (data) {
                data.seek(ptr);
                while (_tagID < _maxTagCount) {

                    byte byteData[] = null;
                    byteData = readTaggedRowColumn(data);
                    if (byteData == null) {
                        break;
                    }
                    if (byteData != null) {
                        String columnValue = new String(byteData);
                        if (columnValue.length() == 0) {
                            columnValue = null;
                        }

                        trySettingColumn(row, _tagID - 1, columnValue);

                        // found invalid data while trying to set column value
                        // so ignore this line
                        if (row == null) {
                            break;
                        }
                    }
                }
                _headerBytesOffset += _recordTrailerByteCount;
            }
            return row;
        } catch (IOException e) {
            e.printStackTrace();
            throw new AxionException(e);
        }
    }

    private byte[] readTaggedRowColumn(BufferedDataInputStream rFile) throws IOException {
        // decode tagID and column length
        // The tagid and column length are Binary encoded
        // use left shift 8 and use the next byte to mask
        // FIXME: Read Left/Right shift from metadata
        _tagBuf = new byte[_tagLength];

        int bytesRead = rFile.read(_tagBuf);
        byte[] byteData = null;
        if (bytesRead == -1) {
            return null; // EOF
        }
        _tagID = (_tagBuf[0] << 8) | _tagBuf[1];
        int colLength = (_tagBuf[_tagByteCount] << 8) | _tagBuf[_tagByteCount + 1];
        _headerBytesOffset += _tagLength;
        if ((_tagID <= _maxTagCount) && (_tagID >= _minTagCount) && colLength >= 0) {
            byteData = new byte[colLength];
            bytesRead = rFile.read(byteData);
            if (bytesRead == -1) {
                return null;
            }

            //Just for testing, convert to ascii
            if (_encoding != null && byteData != null) {
                AsciiEbcdicEncoder.convertEbcdicToAscii(byteData);
            }

            _headerBytesOffset += colLength;

        }
        return byteData;
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
        super.initializeTable();
        _tagBuf = new byte[_tagLength];

    }

    public long getCurrentParsePosition() {
        return _headerBytesOffset;
    }

    private int getColumnSize(int index) {
        return getColumn(index).getDataType().getColumnDisplaySize();
    }

    public boolean loadExternalTable(Properties props) throws AxionException {
        context = new TaggedEBCDICTableContext();
        return super.loadExternalTable(props);
    }

    public Properties getTableProperties() {
        return context.getTableProperties();
    }

    protected void parseTableProperties(ObjectInputStream in) throws AxionException {
        try {
            _tagLength = in.readInt();
            _minTagCount = in.readInt();
            _maxTagCount = in.readInt();
            _recordTrailerByteCount = in.readInt();
            _recordLength = in.readInt();
            _tagByteCount = in.readInt();
            _encoding = in.readUTF();
            _headerBytesOffset = in.readInt();
            _fileName = in.readUTF();

            context = new TaggedEBCDICTableContext();
            context.updateProperties();
            context.readOrSetDefaultProperties(context.getTableProperties());
            createOrLoadDataFile();
        } catch (IOException e) {
            throw new AxionException("Unable to parse meta file for table " + getName(), e);
        }
    }

    protected synchronized void renameTableFiles(String oldName, String name) {
        super.renameTableFiles(oldName, name);
        _recordLength = 0;
        for (int i = 0, I = getColumnCount(); i < I; i++) {
            _recordLength += this.getColumnSize(i);
        }
        context.setProperty(PROP_RECORDLENGTH, Integer.toString(_recordLength));
    }

    protected void writeTableProperties(ObjectOutputStream out) throws AxionException {
        try {
            out.writeInt(_tagLength);
            out.writeInt(_minTagCount);
            out.writeInt(_maxTagCount);
            out.writeInt(_recordTrailerByteCount);
            out.writeInt(_recordLength);
            out.writeInt(_tagByteCount);
            if (_fileName != null && _encoding != null) {
                out.writeUTF(_encoding);
                out.writeInt(_headerBytesOffset);
                out.writeUTF(_fileName);
            }
        } catch (IOException e) {
            throw new AxionException("Unable to write meta file for table " + getName(), e);
        }
    }

    protected void writeHeader(BufferedDataOutputStream dataFile) throws AxionException {
    }

    protected void writeRow(BufferedDataOutputStream out, Row row) throws AxionException {
        byte trailerBytes[] = new byte[_recordTrailerByteCount];
        java.util.Arrays.fill(trailerBytes, (byte) EBCDIC_FILLER);
        try {

            for (int i = 0, I = getColumnCount(); i < I; i++) {
                Object data = row.get(i);
                DataType dataType = getColumn(i).getDataType();
                byte dataBytes[] = null;
                if (data instanceof byte[]) {
                    if (_encoding != null && data != null) {
                        AsciiEbcdicEncoder.convertEbcdicToAscii((byte[]) data);
                    }
                } else {
                    if (_encoding != null && data != null) {
                        dataBytes = dataType.toString(data).getBytes();
                        AsciiEbcdicEncoder.convertEbcdicToAscii(dataBytes);
                    } else {
                        dataBytes = dataType.toString(data).getBytes();
                    }
                }
                row.set(i, data);
                out.write(writeColumn(i, new String(dataBytes)));
            }
            out.write(trailerBytes);
        } catch (Exception e) {
            throw new AxionException(e.getMessage());
        }
    }

    private byte[] writeColumn(int colIndex, String value) {
        if (value == null) {
            value = " ";
        }

        byte byteData[] = new byte[getColumnSize(colIndex)];
        Arrays.fill(byteData, (byte)EBCDIC_FILLER);
        byte colValue[] = value.getBytes();

        // truncate if required
        int len = colValue.length <= byteData.length ? colValue.length : byteData.length;
        System.arraycopy(colValue, 0, byteData, 0, len);
        return byteData;
    }

    protected boolean isEndOfRecord(int recLength, int nextChar, BufferedDataInputStream data) {
        return recLength >= _recordLength || isNewLine(nextChar) || isCarriageReturn(nextChar)
            || isEOF(nextChar);
    }

    protected boolean isCarriageReturn(int nextChar) {
        return nextChar == CR;
    }

    protected boolean isNewLine(int nextChar) {
        return nextChar == NL;
    }

    private class TaggedEBCDICTableContext extends BaseFlatfileTableOrganizationContext {

        public Set getPropertyKeys() {
            Set baseKeys = super.getPropertyKeys();
            Set keys = new HashSet(baseKeys.size() + PROPERTY_KEYS.size());
            keys.addAll(baseKeys);
            keys.addAll(PROPERTY_KEYS);

            return keys;
        }

        public Set getRequiredPropertyKeys() {
            Set baseRequiredKeys = super.getRequiredPropertyKeys();
            Set keys = new HashSet(baseRequiredKeys.size() + REQUIRED_KEYS.size());
            keys.addAll(baseRequiredKeys);
            keys.addAll(REQUIRED_KEYS);

            return keys;
        }

        public void readOrSetDefaultProperties(Properties props) throws AxionException {
            super.readOrSetDefaultProperties(props);

            // Set record length, if not supplied by user then compute it
            try {
                String recLen = props.getProperty(PROP_RECORDLENGTH);
                if (isNullString(recLen)) {
                    _recordLength = 0;
                } else {
                    _recordLength = Integer.parseInt(recLen);
                }
            } catch (NumberFormatException e) {
                _recordLength = 0;
            }

            // compute record length; if not set yet
            if (_recordLength == 0) {
                for (int i = 0, I = getColumnCount(); i < I; i++) {
                    _recordLength += getColumnSize(i);
                }
            }

            String headerBytesOffset = props.getProperty(PROP_HEADERBYTESOFFSET);
            if (isNullString(headerBytesOffset)) {
                _headerBytesOffset = 0;
            } else {
                _headerBytesOffset = Integer.parseInt(headerBytesOffset);
            }

            String tagLen = props.getProperty(PROP_TAGLENGTH);
            if (isNullString(tagLen)) {
                _tagLength = 0;
            } else {
                _tagLength = Integer.parseInt(tagLen);
            }

            String recordTrailerByteCount = props.getProperty(PROP_RECORDTRAILERBYTECOUNT);
            if (isNullString(recordTrailerByteCount)) {
                _recordTrailerByteCount = 0;
            } else {
                _recordTrailerByteCount = Integer.parseInt(recordTrailerByteCount);
            }

            String minTagCount = props.getProperty(PROP_MINTAGCOUNT);
            if (isNullString(minTagCount)) {
                _minTagCount = 0;
            } else {
                _minTagCount = Integer.parseInt(minTagCount);
            }

            String maxTagCount = props.getProperty(PROP_MAXTAGCOUNT);
            if (isNullString(minTagCount)) {
                _maxTagCount = 0;
            } else {
                _maxTagCount = Integer.parseInt(maxTagCount);
            }
            String tagByteCount = props.getProperty(PROP_TAGBYTECOUNT);
            if (isNullString(tagByteCount)) {
                _tagByteCount = 0;
            } else {
                _tagByteCount = Integer.parseInt(tagByteCount);
            }
            _encoding = props.getProperty(PROP_ENCODING);

        }

        public void updateProperties() {
            super.updateProperties();

            _props.setProperty(PROP_LOADTYPE, ExternalTableFactory.TYPE_TAGGEDEBCDIC);
            _props.setProperty(PROP_HEADERBYTESOFFSET, Integer.toString(_headerBytesOffset));
            _props.setProperty(PROP_RECORDLENGTH, Integer.toString(_recordLength));
            _props.setProperty(PROP_TAGLENGTH, Integer.toString(_tagLength));
            _props.setProperty(PROP_MINTAGCOUNT, Integer.toString(_minTagCount));
            _props.setProperty(PROP_MAXTAGCOUNT, Integer.toString(_maxTagCount));
            _props.setProperty(PROP_RECORDTRAILERBYTECOUNT, Integer
                .toString(_recordTrailerByteCount));
            _props.setProperty(PROP_TAGBYTECOUNT, Integer.toString(_tagByteCount));
            _props.setProperty(PROP_ENCODING, _encoding);
        }
    }

    private int _tagID;
    private int _tagLength;
    private int _maxTagCount;
    private int _minTagCount;
    private int _tagByteCount;
    private int _recordTrailerByteCount;
    private int _recordLength;
    private String _encoding;

    protected long _pos;
    protected int _index;
    protected int _count;
    private byte[] _tagBuf = null;

    private int _headerBytesOffset;

}

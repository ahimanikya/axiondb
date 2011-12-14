/*
 *
 * =======================================================================
 * Copyright (c) 2007 Axion Development Team.  All rights reserved.
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
package org.axiondb.io;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Types;
import java.util.Arrays;
import java.util.regex.Pattern;
import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.Row;
import org.axiondb.engine.rows.SimpleRow;
import org.axiondb.types.CharacterType;

/**
 *
 * @author Ahimanikya Satapathy
 */
public class CharStreamTokenizer {

    private static final String EMPTY_STRING = "";
    private static final String COMMA = ",";
    private static final char NL = Character.MAX_VALUE;
    protected static final int EOF = -1;
    protected static final char FILLER = ' ';

    public CharStreamTokenizer(char[] fieldSepChar, char[][] lineSepsChar, char[] qualifierChar, boolean isQuoted) {
        _fieldSepChar = fieldSepChar;
        _lineSepsChar = lineSepsChar;
        _qualifier = new String(qualifierChar);
        _qualifierChar = qualifierChar;
        _isQuoted = isQuoted;
    }

    public CharStreamTokenizer() {
        _lineSepsChar = new char[1][1];
        _lineSepsChar[0] = System.getProperty("line.separator").toCharArray();
    }

    // skip line from current position
    public void skipLine(BufferedDataInputStream data) throws IOException {
        while (!isEndOfRecord(data.read(), data)) {
        // do nothing
        }
    }

    public boolean isEndOfRecord(int nextChar, BufferedDataInputStream data) throws IOException {
        if (EOF == nextChar) {
            return true;
        }
        boolean foundEOL = false;
        for (int k = 0; (k < _lineSepsChar.length && !foundEOL); k++) {
            if (_lineSepsChar[k].length != 0 && _lineSepsChar[k][0] == nextChar) {
                foundEOL = true;
                // Look ahead to see whether the following chars match EOL.
                long lastDataFileOffset = data.getPos();
                for (int i = 1,  I = _lineSepsChar[k].length; i < I; i++) {
                    if (_lineSepsChar[k][i] != (char) data.read()) {
                        data.seek(lastDataFileOffset);
                        foundEOL = false;
                    }
                }
            }
        }
        return foundEOL;
    }

    public String[] readAndSplitLine(BufferedDataInputStream data, long offset, int colCount, boolean trimWhiteSpace) throws AxionException, IOException {
        data.seek(offset);
        String[] row = new String[colCount];

        char[] charArray = readLine(data, offset);
        if (charArray[0] == NL) {
            throw new AxionException("Empty line detected - invalid.");
        }

        DataType[] datatypes = new CharacterType[colCount];
        CharTokenizer charTokenizer = new CharTokenizer(charArray, trimWhiteSpace, datatypes);
        for (int i = 0; i < colCount && charTokenizer.hasMoreTokens(); i++) {
            row[i] = (String) charTokenizer.nextToken(i);
        }
        return row;
    }

    public Row readAndSplitLine(BufferedDataInputStream data, long offset, int colCount, boolean trimWhiteSpace, DataType[] datatypes) throws AxionException, IOException {
        data.seek(offset);
        Row row = new SimpleRow(colCount);

        char[] charArray = readLine(data, offset);
        if (charArray[0] == NL) {
            throw new AxionException("Empty line detected - invalid.");
        }

        CharTokenizer charTokenizer = new CharTokenizer(charArray, trimWhiteSpace, datatypes);
        for (int i = 0; i < colCount && charTokenizer.hasMoreTokens(); i++) {
            row.set(i, charTokenizer.nextToken(i));
        }
        return row;
    }

    public boolean isNumber(int type) {
        switch (type) {
            case Types.INTEGER:
            case Types.NUMERIC:
            case Types.BIGINT:
            case Types.DECIMAL:
            case Types.SMALLINT:
            case Types.DOUBLE:
            case Types.FLOAT:
                return true;
            default:
                return false;
        }
    }

    public Object convert(char[] val, int start, int end, boolean isNumber) throws AxionException {
        int len = end;
        int st = start;

        while ((st < len) && (val[st] <= ' ')) {
            st++;
        }
        while ((st < len) && (val[len - 1] <= ' ')) {
            len--;
        }

        if (st == len) {
            return null;
        }

        if (isNumber) {
            try {
            return new BigDecimal(val, st, len - st);
            } catch (NumberFormatException e){
                throw new AxionException(e);
            }
        }
        return new String(val, st, len - st);
    }
    private char[] _lineCharArray = new char[LINE_CHAR_ARRAY_SIZE];

    private char[] readLine(BufferedDataInputStream data, long fileOffset) throws AxionException {
        Arrays.fill(_lineCharArray, FILLER);
        int recLength = 0;
        try {
            int nextChar;
            data.seek(fileOffset);

            while (true) {
                nextChar = data.read();
                if (isEndOfRecord(nextChar, data)) {
                    _lineCharArray[recLength] = NL;
                    break;
                }

                // ensure capacity
                if ((recLength + 2) > _lineCharArray.length) {
                    char[] newlineCharArray = new char[recLength + 80];
                    System.arraycopy(_lineCharArray, 0, newlineCharArray, 0, _lineCharArray.length);
                    _lineCharArray = newlineCharArray;
                }

                _lineCharArray[recLength++] = ((char) nextChar);
            }
            return _lineCharArray;

        } catch (IOException e) {
            throw new AxionException("Unable to parse data file: ", e);
        }
    }

    protected boolean isEOF(int nextChar) {
        return nextChar == EOF;
    }

    protected boolean isNullString(String str) {
        return (str == null || str.trim().length() == 0);
    }

    protected boolean isNewLine(int nextChar) {
        return nextChar == NL;
    }

    public class CharTokenizer {

        char[] _charArray;
        private int _currentPosition;
        private int _maxPosition;
        private DataType[] _datatypes;
        boolean _trimWhiteSpace;

        public CharTokenizer(char[] thecharArray, boolean trimWhiteSpace, DataType[] datatypes) {
            _charArray = thecharArray;
            _maxPosition = _charArray.length;
            _currentPosition = 0;
            _datatypes = datatypes;
            _trimWhiteSpace = trimWhiteSpace;
        }

        public boolean hasMoreTokens() {
            return (_currentPosition < _maxPosition);
        }

        public Object nextToken(int colIndex) throws AxionException {
            int start = _currentPosition;
            int end = start;
            int pos = _currentPosition;
            boolean inQuotedString = false;
            boolean endQuotedString = false;
            boolean treatAsUnquoted = false;
            boolean wasEscaped = false;

            while (pos < _maxPosition) {
                // if new line
                if (isNewLine(_charArray[pos])) {
                    if (_isQuoted && !endQuotedString) {
                        _maxPosition = pos;
                        _currentPosition = pos;
                        end = pos;
                        break;
                    }
                    _currentPosition = _maxPosition;
                }

                // if quoted and found qualifier
                if (_isQuoted && isQualifier(pos)) {
                    if (!inQuotedString) { // not inside the quoted string
                        pos += _qualifierChar.length;
                        start = pos;
                        inQuotedString = true;
                        continue;
                    } else if (isQualifier(pos + _qualifierChar.length)) {
                        pos += (_qualifierChar.length * 2);
                        wasEscaped = true;
                        continue;
                    }
                    // inside the quoted string
                    end = pos;
                    pos += _qualifierChar.length;
                    inQuotedString = false;
                    endQuotedString = true;
                    continue;
                }

                // if quoted, close quote found, but have not found a delimiter yet
                if (_isQuoted && endQuotedString && _fieldSepChar[0] != _charArray[pos] && !isNewLine(_charArray[pos])) {
                    pos++;
                    continue;
                }

                // if quoted, close quote found and found a delimiter
                if (_isQuoted && endQuotedString) {
                    if (isDelimiter(pos)) {
                        pos += _fieldSepChar.length;
                        break;
                    } else if (isNewLine(_charArray[pos])) {
                        break;
                    }
                }

                // if quoted but did not find start qualifer, treat this token as
                // unquoted
                if (_isQuoted && !inQuotedString) {
                    treatAsUnquoted = true;
                }

                // if non-quoted
                if ((!_isQuoted || treatAsUnquoted) && pos < _maxPosition) {
                    if (isDelimiter(pos)) {
                        end = pos;
                        pos += _fieldSepChar.length;
                        break;
                    } else if (isNewLine(_charArray[pos])) {
                        end = pos;
                        break;
                    }
                }

                pos++;
            }

            _currentPosition = pos;
            if (pos == _maxPosition) {
                end = _maxPosition;
            }

            int jdbcType = _datatypes[colIndex].getJdbcType();
            DataType type = _datatypes[colIndex];


            if (start != end) {
                if (isNumber(jdbcType)) {
                    return convert(_charArray, start, end, true);
                } else if (type instanceof CharacterType || _trimWhiteSpace) {
                    Object token = convert(_charArray, start, end, false);
                    if (wasEscaped) {
                        _qqPattern = Pattern.compile(_qualifier + _qualifier);
                        return _qqPattern.matcher((String) token).replaceAll(_qualifier);
                    }
                    return token;
                } else {
                    return new String(_charArray, start, end - start);
                }

            } else if (endQuotedString) {
                return EMPTY_STRING;
            } else {
                return null;
            }

        }

        // if delimiter more than 1 char long, make sure all chars match
        private boolean isDelimiter(int position) {
            boolean delimiterFound = true;
            for (int j = 0,  J = _fieldSepChar.length; j < J; j++) {
                if (_fieldSepChar[j] != _charArray[position++]) {
                    delimiterFound = false;
                    break;
                }
            }
            return delimiterFound;
        }

        // if qualifier more than 1 char long, make sure all chars match
        private boolean isQualifier(int position) {
            boolean qualifierFound = true;
            for (int j = 0,  J = _qualifierChar.length; j < J; j++) {
                if (_qualifierChar[j] != _charArray[position++]) {
                    qualifierFound = false;
                    break;
                }
            }
            return qualifierFound;
        }
    }
    // Expose default size of _lineCharArray for associated unit test.
    public static final int LINE_CHAR_ARRAY_SIZE = 80;
    private char[][] _lineSepsChar;
    private char[] _fieldSepChar = new String(",").toCharArray();
    private char[] _qualifierChar = EMPTY_STRING.toCharArray();
    private boolean _isQuoted = false;
    private String _qualifier;
    private Pattern _qqPattern;
}

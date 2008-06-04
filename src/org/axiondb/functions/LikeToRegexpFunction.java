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

package org.axiondb.functions;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.FunctionFactory;
import org.axiondb.RowDecorator;
import org.axiondb.types.StringType;

/**
 * @version  
 * @author Chuck Burdick
 * @author Ritesh Adval
 */
public class LikeToRegexpFunction extends BaseRegExpFunction implements ScalarFunction, FunctionFactory {

    public LikeToRegexpFunction() {
        super("LIKE_TO_REGEXP");
    }

    public ConcreteFunction makeNewInstance() {
        return new LikeToRegexpFunction();
    }

    public DataType getDataType() {
        return STRING_TYPE;
    }

    public Object evaluate(RowDecorator row) throws AxionException {
        Object arg = getArgument(0).evaluate(row);
        String likePattern = (String) STRING_TYPE.convert(arg);
        if (likePattern == null) {
            return null;
        }

        char escChar = 0;
        if (getArgumentCount() == 2) {
            Object arg1 = getArgument(1).evaluate(row);
            String origEscChar = (String) STRING_TYPE.convert(arg1);
            if (origEscChar != null) {
                if (origEscChar.length() != 1) {
                    throw new AxionException(22019);
                }
                escChar = origEscChar.charAt(0);
            } else {
                return null;
            }
        }

        String processedLikePattern = (String) getFromCache(likePattern + escChar);

        if (processedLikePattern == null) {
            processedLikePattern = convertLike(likePattern, escChar);;
            putInCache(likePattern + escChar, processedLikePattern);
        }
        return processedLikePattern;
    }

    protected String convertLike(String orig) throws AxionException {
        char ch = 0;
        return convertLike(orig, ch);
    }

    // Per ANSI 92, 99, 2003 specs:
    // If escape character is specified then check if we have escape char not followed by
    // escape character itself or '%' or '_' ; if true then throw exception
    protected String convertLike(String orig, char skipChar) throws AxionException {
        StringBuffer _buf = new StringBuffer(2 * orig.length());

        if (orig.length() == 0 || orig.charAt(0) != ESCAPE_PERCENTAGE) {
            _buf.append("^");
        }

        for (int i = 0, I = orig.length(); i < I; i++) {
            char next = orig.charAt(i);
            if (next == skipChar) {
                int j = i + 1;
                if (j < orig.length()) {
                    // what is the next next char
                    char nextNext = orig.charAt(j);

                    // if next character is a valid SQL escape character then write out the escaped
                    // character.
                    if (isValidSQLEscapedCharacter(nextNext)) {
                        _buf.append(nextNext);
                        i++;
                    // if next character is escape character then write out the escape character 
                    } else if (nextNext == skipChar) {
                        i++;
                        _buf.append(escapeCharForRegex(nextNext));
                    } else {
                        // next character is neither a valid SQL escape character nor a
                        // escape character, this is an error condition
                        throw new AxionException("character following the escape character '" + skipChar 
                            + "' should be either a valid SQL escape character or the escape character itself; problem at " 
                            + orig.substring(i), 22025);
                    }
                    continue;
                } else {
                    // Cannot have a single escape character at the end of a like pattern.
                    throw new AxionException(22025);
                }
            }

            String processed = escapeCharForRegex(next);
            _buf.append(processed);
        }

        if (orig.length() < 2 || (orig.charAt(orig.length() - 1) != ESCAPE_PERCENTAGE && orig.charAt(orig.length() - 2) != '\\')) {
            _buf.append("$");
        }
        return _buf.toString();
    }

    private String escapeCharForRegex(char ch) {
        String repl = null;
        switch (ch) {
            case '%':
                repl = ".*";
                break;
            case '_':
                repl = ".";
                break;
            case '?':
                repl = "\\?";
                break;
            case '*':
                repl = "\\*";
                break;
            case '.':
                repl = "\\.";
                break;
            case '$':
                repl = "\\$";
                break;
            case '\\':
                repl = "\\\\";
                break;
            case '+':
                repl = "\\+";
                break;
            case '^':
                repl = "\\^";
                break;
            case '{':
                repl = "\\{";
                break;
            case '}':
                repl = "\\}";
                break;
            case '[':
                repl = "\\[";
                break;
            case ']':
                repl = "\\]";
                break;
            case '(':
                repl = "\\(";
                break;
            case ')':
                repl = "\\)";
                break;
            case '|':
                repl = "\\|";
                break;
            default:
                repl = String.valueOf(ch);
        }

        return repl;
    }

    private boolean isValidSQLEscapedCharacter(char ch) {
        boolean result = false;
        if (ch == ESCAPE_PERCENTAGE || ch == ESCAPE_UNDERSCORE) {
            result = true;
        }
        return result;
    }

    public boolean isValid() {
        return (getArgumentCount() == 1) || (getArgumentCount() == 2);
    }

    private static final DataType STRING_TYPE = new StringType();

    private static char ESCAPE_PERCENTAGE = '%';
    private static char ESCAPE_UNDERSCORE = '_';
}

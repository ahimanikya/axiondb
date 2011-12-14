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

package org.axiondb.engine.commands;

import java.math.BigInteger;

import org.axiondb.AxionException;
import org.axiondb.DataType;
import org.axiondb.Database;
import org.axiondb.Sequence;

/**
 * A <code>CREATE SEQUENCE</code> command.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class CreateSequenceCommand extends CreateCommand {
    public CreateSequenceCommand() {
    }

    public CreateSequenceCommand(String sequenceName, int startVal) {
        setObjectName(sequenceName);
        _startVal = BigInteger.valueOf(startVal);
    }

    public void setStartValue(String value) {
        _startVal = new BigInteger(value, Sequence.RADIX);
    }

    public void setIncrementBy(String incrementBy) {
        _incrementBy = new BigInteger(incrementBy, Sequence.RADIX);
    }

    public void setMaxValue(String maxValue) {
        _maxValue = new BigInteger(maxValue, Sequence.RADIX);
    }

    public void setMinValue(String minValue) {
        _minValue = new BigInteger(minValue, Sequence.RADIX);
    }

    public void setCycle(boolean cycle) {
        _isCycle = cycle;
    }

    public void setDataType(String typeName) {
        _typeName = typeName;
    }
    
    public void setIdentityType(String type) {
        _identityType = type;
    }
    
    public String getIdentityType() {
        return _identityType;
    }

    public boolean execute(Database db) throws AxionException {
        assertNotReadOnly(db);
        if (!db.hasSequence(getObjectName())) {
            db.createSequence(createSequence(db));
        } else if (!isIfNotExists()) {
            throw new AxionException("A sequence named \"" + getObjectName() + "\" already exists.");
        }
        return false;
    }

    private void validate(DataType type) throws AxionException {
        int precision = type.getPrecision();
        if (_maxValue.toString(10).length() > precision) {
            throw new AxionException("Invalid MaxValue");
        }
    }

    public Sequence createSequence(Database db) throws AxionException {
        DataType type = db.getDataType(_typeName);
        validate(type);

        // if start value is not set use default
        if (_startVal.signum() == -1) {
            if (_incrementBy.signum() == -1) {
                _startVal = _maxValue; // start with maxValue
            } else {
                _startVal = _minValue; // start with minValue
            }
        }

        return new Sequence(getObjectName(), type, _startVal, _incrementBy, _maxValue, _minValue, _isCycle);
    }

    private BigInteger _startVal = BigInteger.valueOf(-1);
    private BigInteger _incrementBy = BigInteger.valueOf(1);
    private BigInteger _maxValue = BigInteger.valueOf(Integer.MAX_VALUE);
    private BigInteger _minValue = BigInteger.valueOf(0);
    private boolean _isCycle = false;
    private String _identityType;
    private String _typeName = "INTEGER";
}

/*
 * 
 * =======================================================================
 * Copyright (c) 2004 Axion Development Team.  All rights reserved.
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
import org.axiondb.util.ValuePool;

/**
 * A <code>ALTER SEQUENCE</code> command.
 * 
 * NOTE: One can't change DataType as per ANSI 2003 spec.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class AlterSequenceCommand extends CreateCommand {
    public AlterSequenceCommand() {
    }

    public void setStartValue(String value) {
        _startVal = value;
    }

    public void setIncrementBy(String incrementBy) {
        _incrementBy = incrementBy;
    }

    public void setMaxValue(String maxValue) {
        _maxValue = maxValue;
    }

    public void setMinValue(String minValue) {
        _minValue = minValue;
    }

    public void setCycle(boolean cycle) {
        _isCycle = ValuePool.getBoolean(cycle);
    }

    public boolean execute(org.axiondb.Database db) throws AxionException {
        assertNotReadOnly(db);

        if (!db.hasSequence(getObjectName())) {
            throw new AxionException("Sequence named \"" + getObjectName() + "\" does not exist.");
        }
        Sequence old = db.getSequence(getObjectName());
        db.createSequence(copyAndUpdateSequenceOptions(db, old));

        return false;
    }

    private BigInteger parseBigInt(String value) {
        return new BigInteger(value, Sequence.RADIX);
    }

    private Sequence copyAndUpdateSequenceOptions(Database db, Sequence old) throws AxionException {
        BigInteger startVal;
        if (_startVal == null) {
            startVal = (BigInteger) old.getValue();
        } else {
            startVal = parseBigInt(_startVal);
        }

        BigInteger incrementBy;
        if (_incrementBy == null) {
            incrementBy = old.getIncrementBy();
        } else {
            incrementBy = parseBigInt(_incrementBy);
        }

        BigInteger maxValue;
        if (_maxValue == null) {
            maxValue = old.getMaxValue();
        } else {
            maxValue = parseBigInt(_maxValue);
        }

        BigInteger minValue;
        if (_minValue == null) {
            minValue = old.getMinValue();
        } else {
            minValue = parseBigInt(_minValue);
        }

        boolean isCycle;
        if (_isCycle == null) {
            isCycle = old.isCycle();
        } else {
            isCycle = _isCycle.booleanValue();
        }

        DataType type = old.getDataType();

        return new Sequence(getObjectName(), type, startVal, incrementBy, maxValue, minValue,
            isCycle);
    }

    private String _startVal;
    private String _incrementBy;
    private String _maxValue;
    private String _minValue;
    private Boolean _isCycle;
}

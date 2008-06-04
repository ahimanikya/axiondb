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

package org.axiondb;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.axiondb.event.DatabaseSequenceEvent;
import org.axiondb.event.SequenceModificationListener;
import org.axiondb.types.IntegerType;

/**
 * A database sequence. A sequence provides a mechanism for obtaining unique integer
 * values from the database.
 * <p>
 * Note: If increment value is negative, then the sequence generator is a descending
 * sequence generator; otherwise, it is an ascending sequence generator.
 * <p>
 * Note: The data type of a sequence generator must be exact numeric with scale 0.
 * 
 * @version  
 * @author Chuck Burdick
 * @author Ahimanikya Satapathy
 */
public class Sequence implements Serializable {

    public Sequence() {
    }

    /**
     * Create a sequence with all ANSI 2003 parameters.
     */
    public Sequence(String name, DataType type, BigInteger startVal, BigInteger incrementBy, BigInteger maxValue, BigInteger minValue, boolean isCycle) {
        _type = type;
        _nextValue = startVal;
        _incrementBy = incrementBy;
        _name = name.toUpperCase();
        _listeners = new ArrayList();
        _maxValue = maxValue;
        _minValue = minValue;
        _isCycle = isCycle;
        assertRules();
    }

    /**
     * Create a sequence starting whose initial value is <i>startVal </i>.
     */
    public Sequence(String name, int startVal) {
        _type = new IntegerType();
        _name = name.toUpperCase();
        _nextValue = BigInteger.valueOf(startVal);
        _listeners = new ArrayList();
    }

    @SuppressWarnings("unchecked")
    public void addSequenceModificationListener(SequenceModificationListener listener) {
        _listeners.add(listener);
    }

    /**
     * Returns <code>true</code> iff <i>otherobject </i> is a {@link Sequence}whose
     * name are equal to mine.
     */
    @Override
    public boolean equals(Object otherobject) {
        if (otherobject instanceof Sequence) {
            Sequence that = (Sequence) otherobject;
            return getName().equals(that.getName());
        }
        return false;
    }

    /**
     * Increment and return the next value in this sequence.
     */
    public Object evaluate() throws AxionException {
        _currValue = _nextValue;
        _nextValue = _nextValue.add(_incrementBy);
        if (_nextValue.compareTo(_minValue) == -1 || _nextValue.compareTo(_maxValue) == 1) {
            if (_isCycle) {
                _nextValue = _incrementBy.signum() == 1 ? _minValue : _maxValue;
            } else {
                throw new IllegalStateException("No more value available for this sequence...");
            }
        }

        Iterator it = _listeners.iterator();
        while (it.hasNext()) {
            SequenceModificationListener cur = (SequenceModificationListener) it.next();
            cur.sequenceIncremented(new DatabaseSequenceEvent(this));
        }
        return getDataType().convert(_currValue);
    }

    public Object getCuurentValue() throws AxionException {
        return getDataType().convert(_currValue);
    }

    public DataType getDataType() {
        return _type;
    }

    public BigInteger getIncrementBy() {
        return _incrementBy;
    }

    public BigInteger getMaxValue() {
        return _maxValue;
    }

    public BigInteger getMinValue() {
        return _minValue;
    }

    /**
     * Get the name of this sequence.
     */
    public String getName() {
        return _name;
    }

    /**
     * Get the current value of this sequence.
     */
    public Object getValue() throws AxionException {
        return _nextValue;
    }

    /**
     * Returns a hash code in keeping with the standard {@link Object#equals equals}/
     * {@link Object#hashCode hashCode}contract.
     */
    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    public boolean isCycle() {
        return _isCycle;
    }

    /**
     * @see #write
     */
    public void read(DataInput in) throws Exception {
        _name = in.readUTF();

        String dtypename = in.readUTF();
        Class clazz = Class.forName(dtypename);
        _type = (DataType) (clazz.newInstance());

        _nextValue = new BigInteger(in.readUTF(), Sequence.RADIX);
        _incrementBy = new BigInteger(in.readUTF(), Sequence.RADIX);
        _maxValue = new BigInteger(in.readUTF(), Sequence.RADIX);
        _minValue = new BigInteger(in.readUTF(), Sequence.RADIX);
        _isCycle = in.readBoolean();
        _listeners = new ArrayList();
    }

    /**
     * Writes the given <i>value </i> to the given <code>DataOutput</code>.
     * 
     * @param value the value to write, which must be {@link Sequence}
     */
    public void write(DataOutput out) throws IOException {
        out.writeUTF(getName());
        out.writeUTF(getDataType().getClass().getName());
        out.writeUTF(_nextValue.toString(Sequence.RADIX));
        out.writeUTF(getIncrementBy().toString(Sequence.RADIX));
        out.writeUTF(getMaxValue().toString(Sequence.RADIX));
        out.writeUTF(getMinValue().toString(Sequence.RADIX));
        out.writeBoolean(isCycle());
    }

    private void assertRules() {
        if (_incrementBy.signum() == 0) {
            throw new IllegalArgumentException("IncrementBy Should be non-zero numeric literal");
        }

        if (_minValue.compareTo(_maxValue) >= 0) {
            throw new IllegalArgumentException("MinValue Should be less than MaxValue");
        }

        if (_nextValue.compareTo(_minValue) == -1 || _nextValue.compareTo(_maxValue) == 1) {
            throw new IllegalArgumentException("StartValue Should be within min and max Value");
        }
    }

    public static int RADIX = 10;

    private BigInteger _currValue;
    private BigInteger _incrementBy = BigInteger.valueOf(1);
    private boolean _isCycle = false;

    private List _listeners = null;
    private BigInteger _maxValue = BigInteger.valueOf(Integer.MAX_VALUE);
    private BigInteger _minValue = BigInteger.valueOf(0);
    private String _name = null;
    private BigInteger _nextValue;
    private DataType _type = null;

    private static final long serialVersionUID = -9173917866159022446L;
}

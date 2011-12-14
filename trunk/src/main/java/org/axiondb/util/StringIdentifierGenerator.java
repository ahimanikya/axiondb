/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2003 Axion Development Team.  All rights reserved.
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

package org.axiondb.util;

import java.net.InetAddress;
import java.util.Random;

/**
 * Generates Unique Id across system
 * @author Ahimanikya Satapathy
 */
public class StringIdentifierGenerator {
    public static StringIdentifierGenerator INSTANCE = new StringIdentifierGenerator();
    private Random _randomForSpace = null;
    private Random _randomForTime = null;

    /**
     * @return String generated new ID
     */
    public synchronized String nextIdentifier() {
        long uniqueInSpace = _randomForSpace.nextLong();
        long uniqueInTime = _randomForTime.nextLong();

        return Long.toHexString(uniqueInSpace).toUpperCase() + "_" 
                + Long.toHexString(uniqueInTime).toUpperCase();
    }
    
    /**
     * @param prefix String to be used as prefix for the ID
     * @return String generated new ID
     */
    public String nextIdentifier(String prefix) {
        return prefix + "_" + nextIdentifier();
    }

    private StringIdentifierGenerator() {
        long uniqueSpaceSeed = createUniqueSpaceSeed();
        long uniqueTimeSeed = System.currentTimeMillis();

        _randomForSpace = new Random(uniqueSpaceSeed);
        _randomForTime = new Random(uniqueTimeSeed);
    }

    /**
     * @return long a seed number based on current state of the local machine
     */
    private long createUniqueSpaceSeed() {
        StringBuffer hBuf = new StringBuffer(20);

        hBuf.append(Runtime.getRuntime().freeMemory());
        hBuf.append(Runtime.getRuntime().maxMemory());
        hBuf.append(Runtime.getRuntime().totalMemory());

        //try to get my IP address and add to the hashingString
        try {
            hBuf.append(InetAddress.getLocalHost());
        } catch (Exception e) {
            //Do nothing
        }

        return hBuf.hashCode();
    }

    /**
     * Generates a new 16-digit (hex) identifier.
     * 
     * @return new 16-digit (hex) identifier
     */
	public synchronized String next16DigitIdentifier() {
        String hexSpaceValue = getNextUniqueInSpace();
		String hexTimeValue = getUniqueInTime();
        
		return "00000000".substring(0, 8 - hexSpaceValue.length()) + hexSpaceValue 
                + "00000000".substring(0, 8 - hexTimeValue.length()) + hexTimeValue;
	}
    
    private String getUniqueInTime() {
        int maskedValue;
        long uniqueInTime = _randomForTime.nextLong();
        maskedValue = (int) (uniqueInTime & 0xFFFFFFFFL);
		String hexTimeValue = Integer.toHexString(maskedValue).toUpperCase();
        return hexTimeValue;
    }

    private String getNextUniqueInSpace() {
        long uniqueInSpace = _randomForSpace.nextLong();
        int maskedValue = (int) (uniqueInSpace & 0xFFFFFFFFL);
	    String hexSpaceValue = Integer.toHexString(maskedValue).toUpperCase();
        return hexSpaceValue;
    }

    /**
     * Generates a new 32-digit (hex) identifier.
     * 
     * @return new 32-digit (hex) identifier
     */
    public synchronized String next32DigitIdentifier() {
        String hexSpaceValue = getNextUniqueInSpace();
        String hexTimeValue = getUniqueInTime();
        
        return "0000000000000000".substring(0, 16 - hexSpaceValue.length()) + hexSpaceValue 
                + "0000000000000000".substring(0, 16 - hexTimeValue.length()) + hexTimeValue;
    }
    
    /**
     * Generates a new 16-digit (hex) identifier with the given String as its prefix.
     * 
     * @return new 16-digit (hex) identifier, prefixed by the given String and an underscore separator.
     */    
    public String next32DigitIdentifier(String prefix) {
        return prefix + "_" + next32DigitIdentifier();
    }

    /**
     * Generates a new 16-digit (hex) identifier with the given String as its prefix.
     * 
     * @return new 16-digit (hex) identifier, prefixed by the given String and an underscore separator.
     */    
	public String next16DigitIdentifier(String prefix) {
		return prefix + "_" + next16DigitIdentifier();
	}

    //    public static void main(String[] args) {
    //        for (int i = 0; i < 1000; i++) {
    //            System.out.println(INSTANCE.next16DigitIdentifier());
    //        }
    //    }
}

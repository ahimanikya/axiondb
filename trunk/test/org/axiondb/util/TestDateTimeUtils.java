/*
 * $Id: TestDateTimeUtils.java,v 1.1 2007/11/28 10:01:51 jawed Exp $
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
package org.axiondb.util;

import org.axiondb.AxionException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit tests for DateTimeUtils function.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:51 $
 * @author Jonathan Giron
 */
public class TestDateTimeUtils extends TestCase {
    public TestDateTimeUtils(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDateTimeUtils.class);
    }
    
    public void testProcessFormatStringWithValidInputs() throws Exception {
        assertEquals("hh", DateTimeUtils.processFormatString("HH"));
        assertEquals("hh", DateTimeUtils.processFormatString("HH12"));
        assertEquals("hh", DateTimeUtils.processFormatString("hh12"));
        assertEquals("HH", DateTimeUtils.processFormatString("HH24"));
        assertEquals("HH", DateTimeUtils.processFormatString("hh24"));
        assertEquals("mmMM", DateTimeUtils.processFormatString("mimm"));
        assertEquals("mmMM", DateTimeUtils.processFormatString("MIMM"));
        assertEquals("mmMM", DateTimeUtils.processFormatString("MImm"));
        assertEquals("mmMM", DateTimeUtils.processFormatString("miMM"));
        assertEquals("yyyy", DateTimeUtils.processFormatString("YYYY"));
        assertEquals("hhMMMM", DateTimeUtils.processFormatString("hhMONTH"));
        assertEquals("DDDyy", DateTimeUtils.processFormatString("DDDYY"));
    }
    
    public void testLabelToCode() throws Exception {
        assertEquals(DateTimeUtils.DAY, DateTimeUtils.labelToCode("day"));
        assertEquals(DateTimeUtils.DAY, DateTimeUtils.labelToCode("Day"));
        assertEquals(DateTimeUtils.DAY, DateTimeUtils.labelToCode("DAY"));
        
        assertEquals(DateTimeUtils.MONTH, DateTimeUtils.labelToCode("month"));
        assertEquals(DateTimeUtils.MONTH, DateTimeUtils.labelToCode("Month"));
        assertEquals(DateTimeUtils.MONTH, DateTimeUtils.labelToCode("MONTH"));
        
        assertEquals(DateTimeUtils.YEAR, DateTimeUtils.labelToCode("year"));
        assertEquals(DateTimeUtils.YEAR, DateTimeUtils.labelToCode("Year"));
        assertEquals(DateTimeUtils.YEAR, DateTimeUtils.labelToCode("YEAR"));
        
        assertEquals(DateTimeUtils.MINUTE, DateTimeUtils.labelToCode("minute"));
        assertEquals(DateTimeUtils.MINUTE, DateTimeUtils.labelToCode("Minute"));
        assertEquals(DateTimeUtils.MINUTE, DateTimeUtils.labelToCode("MINUTE"));
        
        assertEquals(DateTimeUtils.SECOND, DateTimeUtils.labelToCode("second"));
        assertEquals(DateTimeUtils.SECOND, DateTimeUtils.labelToCode("Second"));
        assertEquals(DateTimeUtils.SECOND, DateTimeUtils.labelToCode("SECOND"));
        
        assertEquals(DateTimeUtils.WEEK, DateTimeUtils.labelToCode("week"));
        assertEquals(DateTimeUtils.WEEK, DateTimeUtils.labelToCode("Week"));
        assertEquals(DateTimeUtils.WEEK, DateTimeUtils.labelToCode("WEEK"));
        
        assertEquals(DateTimeUtils.QUARTER, DateTimeUtils.labelToCode("quarter"));
        assertEquals(DateTimeUtils.QUARTER, DateTimeUtils.labelToCode("Quarter"));
        assertEquals(DateTimeUtils.QUARTER, DateTimeUtils.labelToCode("QUARTER"));
        
        assertEquals(DateTimeUtils.MILLISECOND, DateTimeUtils.labelToCode("millisecond"));
        assertEquals(DateTimeUtils.MILLISECOND, DateTimeUtils.labelToCode("Millisecond"));
        assertEquals(DateTimeUtils.MILLISECOND, DateTimeUtils.labelToCode("MILLISECOND"));
        
        final String msg = "Expected AxionException (22006) - invalid interval format"; 
        try {
            DateTimeUtils.labelToCode("blah");
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22006", expected.getSQLState());   
        }
        
        try {
            DateTimeUtils.labelToCode(null);
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22006", expected.getSQLState());   
        }
    }

    public void testGetPartMnemonicFor() throws Exception {
        String[][] partMnemonicPair = new String[][] {
            new String[] { "WEEKDAY", "dow" },
            new String[] { "WEEKDAY3", "dy" },
            new String[] { "WEEKDAYFULL", "day" },
            new String[] { "DAY", "dd" },
            new String[] { "MONTH", "mm" },
            new String[] { "MONTH3", "mon" },
            new String[] { "MONTHFULL", "month" },
            new String[] { "YEAR", "yyyy" },
            new String[] { "YEAR2", "yy" },
            new String[] { "HOUR", "h" },
            new String[] { "HOUR12","hh12" },
            new String[] { "HOUR24", "hh24" },
            new String[] { "MINUTE", "mi" },
            new String[] { "SECOND", "ss" },
            new String[] { "WEEK", "w" },
            new String[] { "QUARTER", "q" },
            new String[] { "MILLISECOND", "ff" },
            new String[] { "AMPM", "am" }
        };
        
        for (int i = 0; i < partMnemonicPair.length; i++) {
            assertEquals(partMnemonicPair[i][1], DateTimeUtils.getPartMnemonicFor(partMnemonicPair[i][0]));
        }
        
        final String msg = "Expected AxionException (22006) - invalid interval format"; 
        try {
            DateTimeUtils.getPartMnemonicFor(null);
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22006", expected.getSQLState());   
        }
        
        try {
            DateTimeUtils.getPartMnemonicFor("unknown");
            fail(msg);
        } catch (AxionException expected) {
            assertEquals(msg, "22006", expected.getSQLState());   
        }
    }
}

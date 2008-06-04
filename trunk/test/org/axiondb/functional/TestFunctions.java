/*
 * $Id: TestFunctions.java,v 1.1 2007/11/28 10:01:31 jawed Exp $
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

package org.axiondb.functional;

import java.sql.SQLException;
import java.sql.Timestamp;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:31 $
 * @author Rodney Waldhoff
 */
public class TestFunctions extends AbstractFunctionalTest {

    //------------------------------------------------------------ Conventional

    public TestFunctions(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestFunctions.class);
    }

    //--------------------------------------------------------------- Lifecycle

    //------------------------------------------------------------------- Tests

    public void testAggregateFunctions() throws Exception {
        _stmt.execute("create table foo ( val int )");

        assertResult(new Object[] { null },"select avg(val) from foo");
        assertResult(new Object[] { null },"select max(val) from foo");
        assertResult(new Object[] { null },"select min(val) from foo");
        assertResult(new Object[] { null },"select sum(val) from foo");
        assertResult(0,"select count(val) from foo");

        _stmt.execute("insert into foo values ( 3 )");
        assertResult(3,"select avg(val) from foo");

        _stmt.execute("insert into foo values ( 2 )");
        _stmt.execute("insert into foo values ( 4 )");
        assertResult(3,"select avg(val) from foo");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 5 )");
        assertResult(3,"select avg(val) from foo");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 1 )");
        _stmt.execute("insert into foo values ( 2 )");
        _stmt.execute("insert into foo values ( 1 )");
        assertResult(15,"select count(val) from foo");
        assertResult(5,"select count(distinct val) from foo");

        try{
            assertResult(5,"select count(distinct *) from foo");
            fail("Expected Exception");
        }catch (Exception e) {
            // expected
        }

        assertResult(2,"select avg(val) from foo");
        assertResult(3,"select avg(distinct val) from foo");

        assertResult(1,"select min(val) from foo");
        assertResult(5,"select max(val) from foo");
    }

    public void testExists() throws Exception {
        _stmt.execute("create table foo ( val int )");
        _stmt.execute("insert into foo values ( 3 )");
        assertResult(1,"select 1 where exists (select * from foo)");
        assertResult(1,"select 1 where exists (select val from foo where val = 3)");
        assertNoRows("select 1 where exists (select val from foo where val = 2)");
    }

    public void testNotExists() throws Exception {
        _stmt.execute("create table foo ( val int )");
        _stmt.execute("insert into foo values ( 3 )");
        assertNoRows("select 1 where not exists (select * from foo)");
        assertNoRows("select 1 where not exists (select val from foo where val = 3)");
        assertResult(1,"select 1 where not exists (select val from foo where val = 2)");
    }

    public void testCoalesce() throws Exception {
        assertResult(1,"select coalesce(null,null,1,null)");
        assertResult(3,"select coalesce(null,3,2,null)");
        assertResult(1,"select coalesce(1)");
        assertNullResult("select coalesce(null)");
        assertNullResult("select coalesce(null,null)");
    }

    public void testDateAdd() throws Exception {
        long time = 1083609680013000L;
        assertObjectResult(new Timestamp(time),"select dateadd(millisecond,0," + time + ")");
        assertObjectResult(new Timestamp(time+2),"select dateadd(millisecond,2," + time + ")");
        assertObjectResult(new Timestamp(time+2*1000),"select dateadd(second,2," + time + ")");
        assertObjectResult(new Timestamp(time+2*60*1000),"select dateadd(minute,2," + time + ")");
        assertObjectResult(new Timestamp(time+2*60*60*1000),"select dateadd(hour,2," + time + ")");
        assertObjectResult(new Timestamp(time+2*24*60*60*1000),"select dateadd(day,2," + time + ")");
        assertObjectResult(new Timestamp(time+2*7*24*60*60*1000),"select dateadd(week,2," + time + ")");
    }

    public void testDateDiff() throws Exception {
        long time1 = 1083609680013000L;
        long time2 = time1 + 2*7*24*60*60*1000;
        assertObjectResult(new Long(2*7*24*60*60*1000),"select datediff(millisecond," + time1 + "," + time2 + ")");
        assertObjectResult(new Long(2*7*24*60*60),"select datediff(second," + time1 + "," + time2 + ")");
        assertObjectResult(new Long(2*7*24*60),"select datediff(minute," + time1 + "," + time2 + ")");
        assertObjectResult(new Long(2*7*24),"select datediff(hour," + time1 + "," + time2 + ")");
        assertObjectResult(new Long(2*7),"select datediff(day," + time1 + "," + time2 + ")");
        assertObjectResult(new Long(2),"select datediff(week," + time1 + "," + time2 + ")");
    }

    public void testDivisionByZero() throws Exception {
        try{
            assertResult(0, "Select 20/0");
            fail("Expected Exception");
        }catch (SQLException e) {
            // expected
        }

        try{
            assertResult(0, "Select 20/0.0");
            fail("Expected Exception");
        }catch (SQLException e) {
            // expected
        }

        assertResult(10000,"Select 1/0.0001");
    }

    /**
     * Test expression as arguments to Substring
     * @throws Exception
     */
    /*28-Nov-2007:public void testSubstring() throws Exception {
        _stmt.execute("create table foo ( emp_no numeric(10) not null, emp_name varchar(100))");
        _stmt.execute("insert into foo values ( 1, 'Helloo world....')");
        this._rset = _stmt.executeQuery("select substring(emp_name || 'some str', length('123'), length('12345')) from foo");
        assertNotNull(_rset);
        assertTrue(_rset.next());
        assertEquals(_rset.getString(1), "lloo ");
    }*/


}

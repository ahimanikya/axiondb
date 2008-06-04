/*
 * $Id: AbstractDbdirTest.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
 * =======================================================================
 * Copyright (c) 2002 Axion Development Team.  All rights reserved.
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

import java.io.File;

import junit.framework.TestCase;

import org.axiondb.io.FileUtil;

/**
 * Utility test environment for tests that require a temporary
 * database directory
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author Chuck Burdick
 */
public abstract class AbstractDbdirTest extends TestCase {
    private File _dbDir = new File(new File("."), "TESTDB");

    public AbstractDbdirTest(String testName) {
        super(testName);
    }

    public void setUp() throws Exception {
        _dbDir.mkdir();
    }

    /**
     * Subclasses should call this so that they clean up the mess they
     * made
     */
    public void tearDown() throws Exception {
        deleteFile(_dbDir);
    }

    public void testDbdir() {
        assertNotNull("Should have a dbdir", getDbdir());
    }

    protected File getDbdir() {
        return _dbDir;
    }

    protected boolean deleteFile(File file) throws Exception {
        return FileUtil.delete(file);
    }
}

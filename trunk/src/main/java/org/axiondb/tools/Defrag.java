/*
 * 
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

package org.axiondb.tools;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.axiondb.engine.DiskDatabase;

/**
 * A utility class for defragmenting an Axion database.
 * Use: <pre>Defrag &lt;database-directory&gt;</pre>
 * The database should not be in use when this program
 * is executed.
 *  
 * @version  
 * @author Rodney Waldhoff
 */
public class Defrag {
    public static void main(String[] args) {
        if(args.length != 1) {
            System.err.println("Defragments a database file");
            System.err.println("Arguments: <database-directory>");            
        } else {
            try {
                defragDatabase(args[0]);
            } catch(Exception e) {
                _log.log(Level.FINE,"Exception while defragmenting database: ", e);
                System.err.println("Fail to defrag...");
            }
        }
    }

    public static void defragDatabase(String databaseDirectory) throws Exception {
        defragDatabase(new File(databaseDirectory));
    }

    public static void defragDatabase(File databaseDirectory) throws Exception {
        DiskDatabase db = new DiskDatabase(databaseDirectory);
        db.defrag();
        db.shutdown();
    }
    
    private static Logger _log = Logger.getLogger(Defrag.class.getName());
}

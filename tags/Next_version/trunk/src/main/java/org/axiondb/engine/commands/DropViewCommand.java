/*
 * 
 * =======================================================================
 * Copyright (c) 2002-2006 Axion Development Team.  All rights reserved.
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

import java.util.List;

import org.axiondb.AxionException;
import org.axiondb.Database;
import org.axiondb.Table;

/**
 * A <code>DROP VIEW</code> command.
 * @version  
 * @author Ahimanikya Satapathy 
 */
public class DropViewCommand extends DropCommand {

    public DropViewCommand(String tableName, boolean exists, boolean cascade) {
        setObjectName(tableName);
        setIfExists(exists);
        setCascade(cascade);
    }
    
    public boolean execute(Database db) throws AxionException {
        assertNotReadOnly(db);
        if(!isIfExists() || db.hasTable(getObjectName())) {
            Table t  = db.getTable(getObjectName());
            if(t!= null && t.getType().equals("VIEW")) {
                dropDepedentViews(db);
                db.dropTable(getObjectName());
            } else {
                throw new AxionException("No view " + getObjectName() + " found");
            }
        }
        return false;
    }

    private void dropDepedentViews(Database db) throws AxionException {
        List depedentViews = db.getDependentViews(getObjectName());
        if (depedentViews.size() > 0) {
            if (isCascade()) {
                db.dropDependentViews(depedentViews);
            } else {
                throw new AxionException("Can't drop view: " + getObjectName() + " has reference in another View...");
            }
        }
    }
}


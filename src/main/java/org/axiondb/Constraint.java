/*
 * 
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

package org.axiondb;

import java.io.Serializable;

import org.axiondb.event.RowEvent;

/**
 * A database constraint, such as UNIQUE or NOT NULL.
 * 
 * @version  
 * @author Rodney Waldhoff
 * @author James Strachan
 */
public interface Constraint extends Serializable {

    /**
     * Evaluate the given <i>event </i> under me. Returns <code>false</code> if the
     * constraint I represent has been violated.
     */
    boolean evaluate(RowEvent event) throws AxionException;
    
    /**
     * Evaluate the given <i>event </i> under me. Returns <code>false</code> if the
     * constraint I represent has been violated.
     */
    boolean evaluate(RowEvent event, RowDecorator dec) throws AxionException;
    
    boolean evaluate(RowIterator oldRows, RowIterator newRows, Table table) throws AxionException;

    /**
     * Get my name.
     */
    String getName();

    /**
     * Get a human-readable descrption of the type of constraint I represent.
     */
    String getType();

    /**
     * Return whether or not I am deferrable.
     */
    boolean isDeferrable();

    /**
     * Return whether or not I am deferred. Deferred constraints are not evaluated until
     * the transaction is committed.
     */
    boolean isDeferred();
    /**
     * Resolve any unresolved {@link org.axiondb.Selectableidentifiers} I may have.
     */
    void resolve(Database db, TableIdentifier table) throws AxionException;

    /**
     * Set whether or not I am deferrable.
     */
    void setDeferrable(boolean deferrable);

    /**
     * Set whether or not I am deferred. Deferred constraints are not evaluated until the
     * transaction is committed. Throws an exception if I am not
     * {@link #isDeferrable deferrable}and <i>deferred </i> is <code>true</code>.
     */
    void setDeferred(boolean deferred) throws AxionException;

    /**
     * Set my name.
     */
    void setName(String name);
}



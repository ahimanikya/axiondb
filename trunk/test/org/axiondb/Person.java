/*
 * $Id: Person.java,v 1.1 2007/11/28 10:01:21 jawed Exp $
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

import java.io.Serializable;

/**
 * A dummy bean that can be used for ObjectType testing.
 * 
 * @version $Revision: 1.1 $ $Date: 2007/11/28 10:01:21 $
 * @author James Strachan
 */
public class Person implements Serializable {

    private String name;
    private int dateOfBirth;
    
    public Person() {
    }            
    
    public Person(String name, int dateOfBirth) {
        this.name = name;
        this.dateOfBirth = dateOfBirth;
    }            
    
    public boolean equals(Object that) {
        if (that instanceof Person) {
            return equals((Person) that);
        }
        return false;
    }
    
    public boolean equals(Person that) {
        if (this.dateOfBirth != that.dateOfBirth) {
            return false;
        }
        if (this.name == that.name) {
            return true;
        }
        if (this.name == null) {
            return false;
        }
        return this.name.equals(that.name);
    }                
    
    public int hashCode() {
        int answer = 1234;
        if (name != null) {
            answer ^= name.hashCode();
        }
        answer ^= dateOfBirth;
        return answer;
    }
                        
    public int getDateOfBirth() {
        return dateOfBirth;
    }

    public String getName() {
        return name;
    }

    public void setDateOfBirth(int dateOfBirth) {
        this.dateOfBirth = dateOfBirth;
    }

    public void setName(String name) {
        this.name = name;
    }
}


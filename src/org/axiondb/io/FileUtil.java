/*
 * 
 * =======================================================================
 * Copyright (c) 2005 Axion Development Team.  All rights reserved.
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

package org.axiondb.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileLock;

import org.axiondb.AxionException;

/**
 * Common file utils.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 * @author Girish Patil
 */
public class FileUtil {
    
    public static void assertFileNotLocked(File file) throws AxionException {
        FileOutputStream fos = null;
        FileLock lock = null;
        try {
            fos = new FileOutputStream(file);
            lock = fos.getChannel().tryLock(0L, Long.MAX_VALUE, false);
            if(lock == null) {
                throw new AxionException("Unable to get a lock for file " + file);
            }
        } catch (FileNotFoundException fnf) {
            //ignore
        } catch (IOException ioe) {
            //ignore
        }  finally {
            try {
                if(lock != null) {
                    lock.release();
                }
                if(fos != null) {
                    fos.close();
                }
            } catch (IOException ioe) {
                // ignore
            }
        }
    }

    /**
     * Get rid of File file, whether a true file or dir.
     */
    public static boolean delete(File file) throws AxionException {
        if (file == null || !file.exists()) {
            return true;
        }
        try {
            if (file.isFile()) {
                return file.delete();
            }
            return fullyDelete(file);
        } catch (IOException e) {
            throw new AxionException("Unable to delete " + file);
        }
    }

    public static long getLength(File file) throws IOException {
        FileInputStream fis = new FileInputStream(file);
        long len = fis.getChannel().size();
        fis.close();
        return len;
    }

    public static void renameFile(File dir, String oldName, String newName) {
        File oldfile = new File(dir, oldName);
        File newfile = new File(dir, newName);
        if (newfile.exists()) {
            newfile.delete();
        }
        oldfile.renameTo(newfile);
    }

    public static void renameFile(File dir, String old, String name, String ext) {
        renameFile(dir, old + ext, name + ext);
    }

    public static void renameToUpperCase(File dir) throws IOException {
        File files[] = dir.listFiles();
        if (files != null) {
            for (int i = 0; i < files.length; i++) {
                files[i].renameTo(new File(files[i].getParentFile(), files[i].getName().toUpperCase()));
                if (files[i].isDirectory()) {
                    renameToUpperCase(files[i]);
                }
            }
        }
    }
    
    /**
     * Truncate file to a given length
     * 
     * @see {@link FileChannel.truncate()}
     */
    public static void truncate(File file, long length) throws AxionException {
        FileOutputStream out = null;
        try {
            out = new FileOutputStream(file, true);
            out.getChannel().truncate(length);
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            try {
                out.close();
            } catch (Exception e) {
            }
        }
    }

    private static boolean fullyDelete(File dir) throws IOException {
        File contents[] = dir.listFiles();
        if (contents != null) {
            for (int i = 0; i < contents.length; i++) {
                if (contents[i].isFile()) {
                    if (!contents[i].delete()) {
                        return false;
                    }
                } else {
                    if (!fullyDelete(contents[i])) {
                        return false;
                    }
                }
            }
        }
        return dir.delete();
    }

    public static String getDirecoryFromPath(String path){
        String ret = null;
        int lastNameSeperator = -1;
        if (path != null){
            lastNameSeperator = path.lastIndexOf(File.separator);
            if (lastNameSeperator == -1){
                lastNameSeperator = path.lastIndexOf("\\");
            }

            if (lastNameSeperator == -1){
                lastNameSeperator = path.lastIndexOf("/");
            }

            if (lastNameSeperator != -1){
                ret = path.substring(0, lastNameSeperator + 1);
            }
        }
        return ret;
    }
}

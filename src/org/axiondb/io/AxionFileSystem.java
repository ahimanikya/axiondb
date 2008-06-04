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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;

import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.collections.primitives.ArrayLongList;
import org.apache.commons.collections.primitives.ArrayIntList;
import org.apache.commons.collections.primitives.ArrayUnsignedIntList;
import org.apache.commons.collections.primitives.IntList;
import org.apache.commons.collections.primitives.LongList;
import org.axiondb.AxionException;
import org.axiondb.util.ExceptionConverter;

/**
 * Axion File System, creates file input/output streams and wraps then into a custom
 * BufferedDataStream, which improves perfermance significantly.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class AxionFileSystem {

    public AxionFileSystem() {
    }

    public void closeInputStream(InputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (Exception e) {
            }
        }
    }

    public void closeOutputStream(OutputStream out) {
        if (out != null) {
            try {
                out.close();
            } catch (Exception t) {
            }
        }
    }

    /**
     * create a new file and wrap wrap the stream with BufferedDataOutputStream which
     * improves perfermance significantly.
     */
    public BufferedDataOutputStream createBufferedDOS(File file) throws AxionException {
        try {
            return new BufferedDataOutputStream(open(file, true));
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }

    public boolean createNewFile(File file) throws AxionException {
        try {
            return file.createNewFile();
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }

    public ObjectOutputStream createObjectOutputSteam(File file) throws IOException {
        return new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    }
    
    public DataOutputStream createDataOutputSteam(File file) throws IOException {
        return new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)));
    }

    public PidxList newPidxList(File file, boolean readonly) throws AxionException {
        return new PidxList(file, readonly);
    }

    /**
     * Open the file in read only mode.
     * @param file
     * @return
     * @throws java.io.IOException
     */
    public AxionInputStream open(File file) throws IOException {
        if (!file.exists()) {
            throw new FileNotFoundException(file.toString());
        }
        return new AxionFileInputStream(file);
    }

    /**
     * Open file in append mode if overwrite is false, otherwise create new file.
     * @param file
     * @param overwrite
     * @return
     * @throws java.io.IOException
     */
    public AxionOutputStream open(File file, boolean overwrite) throws IOException {
        if (file.exists() && overwrite) {
            file.delete();
            file.createNewFile();
        }
        File parent = file.getParentFile();
        if (parent != null) {
            parent.mkdirs();
        }
        return new AxionFileOutputStream(file);
    }

    /**
     * Open file in append mode, position will be set to the end of file. Creates the file
     * if does not exist yet.
     * @param file
     * @return
     * @throws java.io.IOException
     */
    public AxionOutputStream openAppend(File file) throws IOException {
        return open(file, false);
    }

    /**
     * Open file in read only mode, position will be set to 0. seek can be used to perferm
     * random access. This will wrap the stream with BufferedDataInputStream which
     * improves perfermance significantly.
     * @param file
     * @return
     * @throws org.axiondb.AxionException
     */
    public BufferedDataInputStream openBufferedDIS(File file) throws AxionException {
        try {
            return new BufferedDataInputStream(open(file));
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }

    /**
     * Open a outputsteam and points the file pointer to a given start position in the
     * file.
     */
    public BufferedDataOutputStream openBufferedDOS(File file, long startPos) throws AxionException {
        try {
            AxionOutputStream out = open(file, true);
            out.seek(startPos);
            return new BufferedDataOutputStream(out);
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }

    /**
     * Open file in append mode, position will be set to the end of file. Creates the file
     * if does not exist yet. This will wrap the stream with BufferedDataOutputStream
     * which improves perfermance significantly.
     */
    public BufferedDataOutputStream openBufferedDOSAppend(File file, int bufferSize) throws AxionException {
        try {
            AxionOutputStream out = open(file, false);
            out.seek(file.length());
            return new BufferedDataOutputStream(out, bufferSize);
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }

    public ObjectInputStream openObjectInputSteam(File file) throws IOException {
        return new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)));
    }
    
    public DataInputStream openDataInputSteam(File file) throws IOException {
        return new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
    }

    /**
     * Reads a list of int values from a file.
     * 
     * @param file the {@link File}to read from
     */
    public IntList parseIntFile(File file) throws AxionException {
        int count = (int) (file.length() / 4L);
        IntList list = new ArrayIntList(count);
        DataInputStream in = null;
        try {
            in = openBufferedDIS(file);
            for (int i = 0; i < count; i++) {
                list.add(in.readInt());
            }
            return list;
        } catch (IOException e) {
            throw new AxionException("Unable to parse " + file, e);
        } finally {
            closeInputStream(in);
        }
    }

    public PidxList parseLongPidxList(File file, boolean readOnly) throws AxionException {
        int count = (int) (file.length() / 8L);
        return new PidxList(file, readOnly);
    }

    public PidxList parseLongPidx(File file, boolean readOnly) throws AxionException {
        int count = (int) (file.length() / 4L);
        byte[] rawdata = new byte[count * 4];
        readAll(file, rawdata);

        LongList list = new ArrayUnsignedIntList(count);
        for (int i = 0,  m = count * 4; i < m; i += 4) {
            long val = ((((long) rawdata[i + 3]) & 0xFF) + ((((long) rawdata[i + 2]) & 0xFF) << 8) + ((((long) rawdata[i + 1]) & 0xFF) << 16) + ((((long) rawdata[i + 0]) & 0xFF) << 24)) & MAX_LONG;
            list.add(val);
        }
        FileUtil.truncate(file, 0);
        writeAllList(list, file);
        return new PidxList(file, readOnly);
    }

    public void writeAllList(LongList list, File file) throws AxionException {
        try {
            BufferedDataOutputStream _out = openBufferedDOSAppend(file, 1024);
            for (int i = 0,  I = list.size(); i < I; i++) {
                _out.writeLong(list.get(i) & MAX_LONG);
            }
            _out.flush();
            _out.close();
            _out = null;
        } catch (IOException e) {
            throw new AxionException(e);
        }
    }
    public void readAll(File file, byte[] rawdata) throws AxionException {
        InputStream in = null;
        try {
            in = open(file);
            in.read(rawdata);
        } catch (IOException e) {
            throw new AxionException(e);
        } finally {
            closeInputStream(in);
        }
    }

    /**
     * Writes a list of <tt>int</tt> values to a file.
     * 
     * @param file the {@link File}to write to
     */
    public void writeIntFile(File file, IntList list) throws AxionException {
        DataOutputStream out = null;
        try {
            out = createBufferedDOS(file);
            for (int i = 0, I = list.size(); i < I; i++) {
              out.writeInt(list.get(i));
            }
        } catch (IOException e) {
            throw new AxionException("Unable to write to " + file, e);
        } finally {
            closeOutputStream(out);
        }
    }

    /**
     * Updates an UnsignedInt value to a file.
     * 
     * @param raf the {@link File}to append to
     * @param offset the pidx file offset to write
     * @param value data file pointer for a given pidx offset
     */
    public void writeUnsignedInt(BufferedDataOutputStream out, long offset, int value) throws AxionException {
        try {
            out.seek(offset);
            out.writeInt(value);
        } catch (IOException e) {
            throw new AxionException("Unable to write to " + out, e);
        }
    }
    
    public void writeToLong(BufferedDataOutputStream out, long offset, long value) throws AxionException {
        try {
            out.seek(offset);
            out.writeLong(value);
        } catch (IOException e) {
            throw new AxionException("Unable to write to " + out, e);
        }
    }
    
    /**
     * Writes a list of <tt>long</tt> values to a file.
     * 
     * @param file the {@link File}to write to
     */
    public void writeUnsignedIntFile(File file, LongList list) throws AxionException {
        DataOutputStream out = null;
        try {
            out = createBufferedDOS(file);
            for (int i = 0, I = list.size(); i < I; i++) {
                out.writeLong(list.get(i) & MAX_LONG);
            }
        } catch (IOException e) {
            throw new AxionException("Unable to write to " + file, e);
        } finally {
            closeOutputStream(out);
        }
    }
    
    /**
     * Writes a list of <tt>long</tt> values to a file.
     *
     * @param file the {@link File}to write to
     */
    public void writeLongFile(File file, LongList list) throws AxionException {
        DataOutputStream out = null;
        try {
            out = createBufferedDOS(file);
            for (int i = 0, I = list.size(); i < I; i++) {
                out.writeLong(list.get(i) & MAX_LONG);
            }
        } catch (IOException e) {
            throw new AxionException("Unable to write to " + file, e);
        } finally {
            closeOutputStream(out);
        }
    }
    
    public long readLongFile(BufferedDataInputStream in, long offset)throws AxionException {
        try{
            in.seek(offset);
            return in.readLong();
        }catch (IOException e) {
            throw new AxionException("AxionFileSystem->Unable to read from " + in, e);
        }
    }

    public class PidxList {
        private BufferedDataOutputStream _out = null;
        private BufferedDataInputStream _in = null;
        AtomicBoolean _isDirty = new AtomicBoolean(true);
        File _pidxFile;
        int _size=0;
        public PidxList(File pidxFile, boolean readOnly) throws AxionException {
            openPidxFile(pidxFile, readOnly);
            _pidxFile = pidxFile;
            _size = (int) (_pidxFile.length()/ 8L);
        }

        public void add(long dataFileOffset) {
            try {
                writeToLong(_out, this.size() * (8L), (dataFileOffset & MAX_LONG));
                _isDirty.set(true);
                _size++;
            } catch (AxionException e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }

        public void close() throws IOException {
            if(_out != null) {
                _out.close();
                _out = null;
            }
            
            if(_in != null) {
                _in.close();
                _in = null;
            }
        }

        public void flush() throws IOException {
            _out.flush();
        }

        public synchronized long get(int index) {
            try {
                if(_isDirty.get()){
                    flush();
                    if(_in != null) {
                        _in.close();
                        _in = null;
                    }
                    _in = openBufferedDIS(_pidxFile);
                    _isDirty.set(false);
                }
                return readLongFile(_in, index * (8L));
            } catch (Exception e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }

        public void set(int rowid, long dataFileOffset) {
            try {
                writeToLong(_out, rowid * (8L), (dataFileOffset & MAX_LONG));
                _isDirty.set(true);
            } catch (Exception e) {
                throw ExceptionConverter.convertToRuntimeException(e);
            }
        }

        public int size() {
            return _size;
        }
        
        private synchronized void openPidxFile(File pidxFile, boolean readOnly) throws AxionException {
            if (!readOnly) {
                _out = openBufferedDOSAppend(pidxFile, 1024);
            }
        }
    }

    // TODO: Experiment with ByteBuffer, that might improve performance.
    // @see FileChannel.read(ByteBuffer src, long position)
    class AxionFileInputStream extends AxionInputStream {

        FileInputStream _fis;

        public AxionFileInputStream(File f) throws IOException {
            this._fis = new FileInputStream(f);
        }

        @Override
        public int available() throws IOException {
            return _fis.available();
        }

        @Override
        public void close() throws IOException {
            _fis.close();
        }

        public long getPos() throws IOException {
            return _fis.getChannel().position();
        }

        public int read() throws IOException {
            return _fis.read();
        }

        @Override
        public int read(byte[] b) throws IOException {
            return _fis.read(b);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            return _fis.read(b, off, len);
        }

        public void seek(long pos) throws IOException {
            _fis.getChannel().position(pos);
        }

        @Override
        public long skip(long n) throws IOException {
            return _fis.skip(n);
        }
        
        public FileInputStream getFileStream() {
            return _fis;
        }
    }

    // TODO: Experiment with ByteBuffer, that might improve performance.
    // @see FileChannel.write(ByteBuffer src, long position)
    class AxionFileOutputStream extends AxionOutputStream {
        private RandomAccessFile _raf;

        public AxionFileOutputStream(File file) throws IOException {
            _raf = new RandomAccessFile(file, "rw");
        }

        @Override
        public void close() throws IOException {
            _raf.close();
        }

        @Override
        public void flush() throws IOException {
        }

        public long getPos() throws IOException {
            return _raf.getFilePointer();
        }

        public void seek(long pos) throws IOException {
            _raf.seek(pos);
        }

        public void truncate(long length) throws IOException {
            _raf.setLength(length);
        }

        @Override
        public void write(byte[] b) throws IOException {
            _raf.write(b);
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            _raf.write(b, off, len);
        }

        public void write(int b) throws IOException {
            _raf.write(b);
        }
    }
    
    private static final long MAX_LONG = Long.MAX_VALUE;

}

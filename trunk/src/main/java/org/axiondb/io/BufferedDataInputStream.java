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
import java.io.DataInputStream;
import java.io.FilterInputStream;
import java.io.IOException;

/**
 * Utility that wraps a {@link AxionInputStream}in a {@link DataInputStream}and buffers
 * input through a {@link BufferedInputStream}.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class BufferedDataInputStream extends DataInputStream {

    /** Buffer input. This improves performance significantly. */
    private static class Buffer extends BufferedInputStream {
        public Buffer(PositionCache in, int bufferSize) throws IOException {
            super(in, bufferSize);
        }

        public long getPos() throws IOException { // adjust for buffer
            return ((PositionCache) in).getPos() - (this.count - this.pos);
        }

        // optimized version of read()
        public int read() throws IOException {
            if (pos >= count)
                return super.read();
            return buf[pos++] & 0xff;
        }

        public synchronized void reset() throws IOException {
            // invalidate buffer
            this.count = 0;
            this.pos = 0;
        }

        public void seek(long desired) throws IOException {
            long current = getPos();
            long start = (current - this.pos);
            if (desired >= start && desired < start + this.count) {
                this.pos += (desired - current); // can position within buffer
            } else {
                this.count = 0; // invalidate buffer
                this.pos = 0;
                ((PositionCache) in).seek(desired); // seek underlying stream
            }
        }
    }

    /** Cache the file position. This improves performance significantly. */
    private static class PositionCache extends FilterInputStream {
        long position;

        public PositionCache(AxionInputStream in) throws IOException {
            super(in);
            this.position = in.getPos();
        }

        public long getPos() throws IOException {
            return position; // return cached position
        }

        // This is the only read() method called by BufferedInputStream, so we trap
        // calls to it in order to cache the position.
        public int read(byte b[], int off, int len) throws IOException {
            int result = in.read(b, off, len);
            position += result;
            return result;
        }

        public void seek(long desired) throws IOException {
            ((AxionInputStream) in).seek(desired); // seek underlying stream
            position = desired; // update position
        }
    }

    public BufferedDataInputStream(AxionInputStream in) throws IOException {
        this(in, 4096);
    }

    public BufferedDataInputStream(AxionInputStream in, int bufferSize) throws IOException {
        super(new Buffer(new PositionCache(in), bufferSize));
    }

    public long getPos() throws IOException {
        return ((Buffer) in).getPos();
    }

    public void seek(long desired) throws IOException {
        ((Buffer) in).seek(desired);
    }
}

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

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;

/**
 * Utility that wraps a {@link AxionOutputStream}in a {@link DataOutputStream}and
 * buffers output through a {@link BufferedOutputStream}.
 * 
 * @version  
 * @author Ahimanikya Satapathy
 */
public class BufferedDataOutputStream extends DataOutputStream {

    private static class Buffer extends BufferedOutputStream {
        private int position;

        public Buffer(PositionCache out, int bufferSize) throws IOException {
            super(out, bufferSize);
        }

        public void flush() throws IOException {
            super.flush();
            position = 0; // invalidate the buffer
        }

        public long getPosition() throws IOException {
            return ((PositionCache) out).getPos() + this.position;
        }

        public void seek(long desired) throws IOException {
            long start = ((PositionCache) out).getPos();
            long current = start + this.position;
            if (desired >= start && desired < start + buf.length) {
                this.position += (desired - current); // can position within buffer
            } else {
                if (count > 0) {
                    flush(); // flush the buffer
                }
                ((PositionCache) out).seek(desired); // seek underlying stream
            }
        }

        public void write(byte b[], int off, int len) throws IOException {
            if (len >= buf.length) {
                flush();
                out.write(b, off, len);
                this.position = 0;
                return;
            }

            if (len > buf.length - count) {
                flush();
                this.position = 0;
            }

            System.arraycopy(b, off, buf, position, len);
            position += len;
            if (position > count) {
                count = position;
            }
        }

        // optimized version of write(int)
        public void write(int b) throws IOException {
            if (count >= buf.length) {
                super.write(b);
                this.position = 1;
            } else {
                buf[position++] = (byte) b;
                if (position > count) {
                    count = position;
                }
            }
        }

    }

    private static class PositionCache extends FilterOutputStream {
        long position;

        public PositionCache(AxionOutputStream out) throws IOException {
            super(out);
            this.position = out.getPos();
        }

        public long getPos() throws IOException {
            return position; // return cached position
        }

        public void seek(long pos) throws IOException {
            ((AxionOutputStream) out).seek(pos);
            position = pos;
        }

        // This is the only write() method called by BufferedOutputStream, so we
        // trap calls to it in order to cache the position.
        public void write(byte b[], int off, int len) throws IOException {
            out.write(b, off, len);
            position += len; // update position
        }
    }

    public BufferedDataOutputStream(AxionOutputStream out) throws IOException {
        this(out, 512);
    }

    public BufferedDataOutputStream(AxionOutputStream out, int bufferSize) throws IOException {
        super(new Buffer(new PositionCache(out), bufferSize));
    }

    public long getPos() throws IOException {
        return ((Buffer) out).getPosition();
    }

    public void seek(long pos) throws IOException {
        ((Buffer) out).seek(pos);
    }

}

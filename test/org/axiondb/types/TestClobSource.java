/*
 * Created on May 23, 2003
 *
 */
package org.axiondb.types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.AxionException;

/**
 * @author mdelagrange
 *
 */
public class TestClobSource extends TestCase {

    /**
     * Constructor for TestClobSource.
     * @param arg0
     */
    public TestClobSource(String arg0) {
        super(arg0);
    }
    
    public static Test suite() {
        return new TestSuite(TestClobSource.class);
    }
    
    public void testUtf8GetCharacterStream() throws Exception {
        ClobSource source = new ClobSource(new TestLobSource());
        Reader reader = source.getCharacterStream();
        char theChar = (char) reader.read();
        assertEquals("should be the right character", '\uFFFA', theChar);
        assertTrue("stream should be exhausted", reader.read() == -1);
    }
    
    public void testUtf8GetAsciiStream() throws SQLException {
        ClobSource source = new ClobSource(new TestLobSource());
        InputStream stream = source.getAsciiStream();
        try {
            stream.read();
            fail("Expected IOException reading a non-ascii stream");
        } catch (IOException e) {
            assertEquals("Could not convert stream from UTF-8 to US-ASCII", e.getMessage());
        }
        
        try {
            stream.close();
        } catch (IOException e1) {
        }
    }
    
    public class TestLobSource implements LobSource {
        public String lob = String.valueOf('\uFFFA');
        
        public long length() throws AxionException {
            try {
                return lob.getBytes("UTF8").length;
            } catch (UnsupportedEncodingException e) {
                throw new AxionException(e);
            }
        }
        
        public void truncate(long length) throws AxionException {
            throw new AxionException();
        }
        
        public InputStream getInputStream() throws AxionException {
            try {
                return new ByteArrayInputStream(lob.getBytes("UTF8"));
            } catch (UnsupportedEncodingException e) {
                throw new AxionException(e.toString());
            }
        }
        
        public OutputStream setOutputStream(long pos) throws AxionException {
            throw new AxionException();
        }
    }

}

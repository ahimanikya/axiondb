/*
 * Created on May 27, 2003
 *
 */
package org.axiondb.types;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.axiondb.util.Utf8InputStreamConverter;

/**
 * @author mdelagrange
 *
 */
public class TestUtf8StreamConverter extends TestCase {

    /**
     * Constructor for TestUtf8StreamConverter.
     * @param arg0
     */
    public TestUtf8StreamConverter(String arg0) {
        super(arg0);
    }
    
    public static Test suite() {
        return new TestSuite(TestUtf8StreamConverter.class);
    }
    
    public void testConstructor() throws UnsupportedEncodingException {
        new Utf8InputStreamConverter(null, "US-ASCII");
    }
    
    public void testConstructorWithUnsupportedEncoding() throws UnsupportedEncodingException {
        try {
            new Utf8InputStreamConverter(null, "foo");
            fail("Expected UnsupportedEncodingException");
        } catch (UnsupportedEncodingException e) {
            assertEquals("foo", e.getMessage());
        }
    }
    

    public void testBadStream() throws UnsupportedEncodingException {
        char theChar = '\uFFFA';
        byte[] bytes = String.valueOf(theChar).getBytes("UTF8");
        
        InputStream stream = new Utf8InputStreamConverter(new ByteArrayInputStream(bytes), "US-ASCII");
        try {
            stream.read();
            fail("should have thrown an exception for a non-ascii stream");
        } catch (IOException e) {
            assertEquals("Could not convert stream from UTF-8 to US-ASCII", e.getMessage());
        }
    }
    
    public void testGoodStream() throws IOException {
        char theChar = '\u0030';
        byte[] bytes = String.valueOf(theChar).getBytes("UTF8");
        
        InputStream stream = new Utf8InputStreamConverter(new ByteArrayInputStream(bytes), "US-ASCII");
        
        int theByte = stream.read();
        assertEquals(48, theByte);
        
        theByte = stream.read();
        assertEquals(-1, theByte);

    }

}

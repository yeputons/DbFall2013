package net.yeputons.cscenter.dbfall2013.util;

import java.io.IOException;
import java.io.InputStream;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 18.10.13
 * Time: 15:58
 * To change this template use File | Settings | File Templates.
 */
public class DataInputStream extends java.io.DataInputStream {
    /**
     * Creates a DataInputStream that uses the specified
     * underlying InputStream.
     *
     * @param in the specified input stream
     */
    public DataInputStream(InputStream in) {
        super(in);
    }

    public byte[] readArray() throws IOException {
        int len = readInt();
        if (len == -1) return null;
        byte[] res = new byte[len];
        readFully(res);
        return res;
    }
}

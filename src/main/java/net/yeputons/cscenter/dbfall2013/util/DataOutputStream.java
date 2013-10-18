package net.yeputons.cscenter.dbfall2013.util;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 18.10.13
 * Time: 16:01
 * To change this template use File | Settings | File Templates.
 */
public class DataOutputStream extends java.io.DataOutputStream {
    /**
     * Creates a new data output stream to write data to the specified
     * underlying output stream. The counter <code>written</code> is
     * set to zero.
     *
     * @param out the underlying output stream, to be saved for later
     *            use.
     * @see java.io.FilterOutputStream#out
     */
    public DataOutputStream(OutputStream out) {
        super(out);
    }

    public void writeArray(byte[] data) throws IOException {
        if (data == null) {
            writeInt(-1);
        } else {
            writeInt(data.length);
            write(data);
        }
    }
}

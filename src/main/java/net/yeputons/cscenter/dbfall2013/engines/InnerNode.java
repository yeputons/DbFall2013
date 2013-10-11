package net.yeputons.cscenter.dbfall2013.engines;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 10.10.13
 * Time: 13:08
 * To change this template use File | Settings | File Templates.
 */
public class InnerNode extends TrieNode {
    int[] next;

    public InnerNode(ByteBuffer buf, int offset) throws IOException {
        super(buf, offset);

        buf.position(offset + 1);
        next = new int[256];
        for (int i = 0; i < next.length; i++)
            next[i] = buf.getInt();
    }

    public static int estimateSize() {
        return 1 + 256 * 4;
    }

    public static InnerNode writeToBuffer(ByteBuffer buf, int offset) throws IOException {
        buf.position(offset);
        buf.put((byte)NODE_INNER);
        for (int i = 0; i < 256; i++)
            buf.putInt(0);
        return new InnerNode(buf, offset);
    }

    public void setNext(int c, int newOffset) throws IOException {
        if (!(0 <= c && c < 256))
            throw new InvalidParameterException("c should be between 0 and 255");
        buf.putInt(offset + 1 + c * 4, newOffset);
        next[c] = newOffset;
    }
}

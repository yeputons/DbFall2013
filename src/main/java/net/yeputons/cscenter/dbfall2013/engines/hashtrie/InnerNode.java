package net.yeputons.cscenter.dbfall2013.engines.hashtrie;

import net.yeputons.cscenter.dbfall2013.util.HugeMappedFile;

import java.io.IOException;
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
    long[] next;

    public InnerNode(HugeMappedFile buf, long offset) throws IOException {
        super(buf, offset);

        buf.position(offset + HEADER_SIZE);
        next = new long[256];
        for (int i = 0; i < next.length; i++)
            next[i] = buf.getLong();
    }

    public static int estimateSize() {
        return HEADER_SIZE + 256 * 8;
    }

    public static InnerNode writeToBuffer(HugeMappedFile buf, long offset) throws IOException {
        buf.position(offset);
        buf.put((byte)NODE_INNER);
        for (int i = 0; i < 256; i++)
            buf.putLong(0);
        return new InnerNode(buf, offset);
    }

    public void setNext(int c, long newOffset) throws IOException {
        if (!(0 <= c && c < 256))
            throw new InvalidParameterException("c should be between 0 and 255");
        buf.putLong(offset + HEADER_SIZE + c * 8, newOffset);
        next[c] = newOffset;
    }
}

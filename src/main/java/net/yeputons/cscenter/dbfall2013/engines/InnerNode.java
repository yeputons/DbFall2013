package net.yeputons.cscenter.dbfall2013.engines;

import java.io.IOException;
import java.io.RandomAccessFile;
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

    public InnerNode(RandomAccessFile file, int offset) throws IOException {
        super(file, offset);

        this.file = file;
        this.offset = offset;

        file.seek(offset + 1);
        next = new int[256];
        for (int i = 0; i < next.length; i++)
            next[i] = file.readInt();
    }

    public static InnerNode addToFile(RandomAccessFile file) throws IOException {
        int offset = (int)file.length();
        file.seek(offset);
        file.writeByte(NODE_INNER);
        for (int i = 0; i < 256; i++)
            file.writeInt(0);
        return new InnerNode(file, offset);
    }

    public void setNext(int c, int newOffset) throws IOException {
        if (!(0 <= c && c < 256))
            throw new InvalidParameterException("c should be between 0 and 255");
        file.seek(offset + 1 + c * 4);
        file.writeInt(newOffset);
        next[c] = newOffset;
    }
}

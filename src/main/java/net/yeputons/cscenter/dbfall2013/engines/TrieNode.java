package net.yeputons.cscenter.dbfall2013.engines;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.security.InvalidParameterException;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 10.10.13
 * Time: 13:01
 * To change this template use File | Settings | File Templates.
 */
abstract class TrieNode {
    public static final int NODE_INNER = 1;
    public static final int NODE_LEAF = 2;

    protected RandomAccessFile file;
    protected int offset;

    protected TrieNode(RandomAccessFile file, int offset) {
        this.file = file;
        this.offset = offset;
    }


    public static TrieNode createFromFile(RandomAccessFile file, int offset) throws IOException {
        file.seek(offset);
        byte type = file.readByte();
        switch (type) {
            case NODE_INNER:
                return new InnerNode(file, offset);
            case NODE_LEAF:
                return new LeafNode(file, offset);
        }
        throw new InvalidParameterException("Unknown node type: " + type);
    }
}

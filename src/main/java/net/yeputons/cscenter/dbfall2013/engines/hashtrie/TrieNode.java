package net.yeputons.cscenter.dbfall2013.engines.hashtrie;

import net.yeputons.cscenter.dbfall2013.util.HugeMappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 10.10.13
 * Time: 13:01
 * To change this template use File | Settings | File Templates.
 */
abstract class TrieNode {
    public static final int HEADER_SIZE = 1;
    public static final int NODE_INNER = 1;
    public static final int NODE_LEAF = 2;

    protected HugeMappedFile buf;
    protected long offset;

    protected TrieNode(HugeMappedFile buf, long offset) {
        this.buf = buf;
        this.offset = offset;
    }


    public static TrieNode createFromFile(HugeMappedFile buf, long offset) throws IOException {
        byte type = buf.get(offset);
        switch (type) {
            case NODE_INNER:
                return new InnerNode(buf, offset);
            case NODE_LEAF:
                return new LeafNode(buf, offset);
        }
        throw new InvalidParameterException("Unknown node type: " + type);
    }
}

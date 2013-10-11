package net.yeputons.cscenter.dbfall2013.engines.hashtrie;

import net.yeputons.cscenter.dbfall2013.util.HugeMappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 10.10.13
 * Time: 13:09
 * To change this template use File | Settings | File Templates.
 */
public class LeafNode extends TrieNode {
    public ByteBuffer key, value;

    public LeafNode(HugeMappedFile buf, long offset) throws IOException {
        super(buf, offset);

        buf.position(offset + HEADER_SIZE);
        int valueLen = buf.getInt();
        if (valueLen == -1) {
            value = null;
        } else {
            value = ByteBuffer.allocate(valueLen);
            buf.get(value.array());
        }

        int keyLen = buf.getInt();
        key = ByteBuffer.allocate(keyLen);
        buf.get(key.array());
    }

    public static int estimateSize(ByteBuffer key, ByteBuffer value) {
        return HEADER_SIZE + 4 + value.limit() + 4 + key.limit();
    }

    public static LeafNode writeToBuffer(HugeMappedFile buf, long offset, ByteBuffer key, ByteBuffer value) throws IOException {
        buf.position(offset);
        buf.put((byte)NODE_LEAF);
        buf.putInt(value.limit());
        buf.put(value.array());
        buf.putInt(key.limit());
        buf.put(key.array());
        return new LeafNode(buf, offset);
    }

    void setValue(ByteBuffer newValue) throws IOException {
        if (newValue != null && (value == null || newValue.limit() > value.limit()))
            throw new ValueIsBiggerThanOldException();
        buf.position(offset + HEADER_SIZE);
        if (newValue != null) {
            buf.putInt(newValue.limit());
            buf.put(newValue.array());
        } else {
            buf.putInt(-1);
        }
        buf.putInt(key.limit());
        buf.put(key.array());
        value = newValue;
    }
}

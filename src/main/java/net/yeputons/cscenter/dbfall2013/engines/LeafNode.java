package net.yeputons.cscenter.dbfall2013.engines;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.security.InvalidParameterException;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 10.10.13
 * Time: 13:09
 * To change this template use File | Settings | File Templates.
 */
public class LeafNode extends TrieNode {
    public ByteBuffer key, value;

    public LeafNode(RandomAccessFile file, int offset) throws IOException {
        super(file, offset);

        file.seek(offset + 1);
        int valueLen = file.readInt();
        if (valueLen == -1) {
            value = null;
        } else {
            value = ByteBuffer.allocate(valueLen);
            file.read(value.array());
        }

        int keyLen = file.readInt();
        key = ByteBuffer.allocate(keyLen);
        file.read(key.array());
    }

    public static int estimateSize(ByteBuffer key, ByteBuffer value) {
        return 1 + 4 + value.limit() + 4 + key.limit();
    }

    public static LeafNode writeToFile(RandomAccessFile file, int offset, ByteBuffer key, ByteBuffer value) throws IOException {
        file.seek(offset);
        file.writeByte(NODE_LEAF);
        file.writeInt(value.limit());
        file.write(value.array());
        file.writeInt(key.limit());
        file.write(key.array());
        return new LeafNode(file, offset);
    }

    void setValue(ByteBuffer newValue) throws IOException {
        if (newValue != null && (value == null || newValue.limit() > value.limit()))
            throw new ValueIsBiggerThanOldException();
        file.seek(offset + 1);
        if (newValue != null) {
            file.writeInt(newValue.limit());
            file.write(newValue.array());
        } else {
            file.writeInt(-1);
        }
        file.writeInt(key.limit());
        file.write(key.array());
        value = newValue;
    }
}

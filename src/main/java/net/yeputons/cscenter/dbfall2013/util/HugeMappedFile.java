package net.yeputons.cscenter.dbfall2013.util;

import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 11.10.13
 * Time: 21:21
 * To change this template use File | Settings | File Templates.
 */
public class HugeMappedFile {
    static final int MAP_STEP_K = 30;
    static final int MAP_STEP = 1 << MAP_STEP_K;
    static final int MAP_STEP_MSK = MAP_STEP - 1;
    protected FileChannel ch;
    protected ArrayList<MappedByteBuffer> bufs;
    protected long currentPosition;

    public HugeMappedFile(FileChannel ch) throws IOException {
        this.ch = ch;
        updateMapping();
        currentPosition = 0;
    }

    void updateMapping() throws IOException {
        bufs = new ArrayList<MappedByteBuffer>();
        for (long i = 0; i < ch.size();) {
            long size = Math.min(MAP_STEP, ch.size() - i);
            bufs.add(ch.map(FileChannel.MapMode.READ_WRITE, i, size));
            i += size;
        }
    }

    public void force() {
        for (int i = 0; i < bufs.size(); i++)
            bufs.get(i).force();
    }

    protected MappedByteBuffer getMap(long position) {
        long mapId = position >> MAP_STEP_K;
        if (mapId < 0 || mapId >= bufs.size())
            throw new IndexOutOfBoundsException();

        long inMapPos = position & (MAP_STEP_MSK - 1);
        assert inMapPos >= 0;
        if (inMapPos >= bufs.get((int)mapId).limit())
            throw new IndexOutOfBoundsException();
        return bufs.get((int)mapId);
    }

    public byte get(long position) {
        return getMap(position).get((int)(position & MAP_STEP_MSK));
    }
    private void put(long position, byte value) {
        getMap(position).put((int)(position & MAP_STEP_MSK), value);
    }

    private void get(long position, byte[] value) {
        for (int i = 0; i < value.length; i++)
            value[i] = get(position + i);
    }
    private void put(long position, byte[] value) {
        for (int i = 0; i < value.length; i++)
            put(position + i, value[i]);
    }

    private int getInt(long position) {
        int res = 0;
        res |= (get(position + 0) & 0xFF) << 24;
        res |= (get(position + 1) & 0xFF) << 16;
        res |= (get(position + 2) & 0xFF) <<  8;
        res |= (get(position + 3) & 0xFF) <<  0;
        return res;
    }
    public void putInt(long position, int value) {
        put(position + 0, (byte)((value >> 24) & 0xFF));
        put(position + 1, (byte)((value >> 16) & 0xFF));
        put(position + 2, (byte)((value >>  8) & 0xFF));
        put(position + 3, (byte)((value >>  0) & 0xFF));
    }

    // Methods which use currentPosition
    public void position(long newPosition) {
        currentPosition = newPosition;
    }

    public int getInt() {
        int res = getInt(currentPosition);
        currentPosition += 4;
        return res;
    }

    public void putInt(int value) {
        putInt(currentPosition, value);
        currentPosition += 4;
    }

    public byte get() {
        byte res = get(currentPosition);
        currentPosition += 1;
        return res;
    }
    public void put(byte value) {
        put(currentPosition, value);
        currentPosition += 1;
    }

    public void get(byte[] value) {
        get(currentPosition, value);
        currentPosition += value.length;
    }

    public void put(byte[] value) {
        put(currentPosition, value);
        currentPosition += value.length;
    }
}

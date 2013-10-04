package net.yeputons.cscenter.dbfall2013.engines;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 04.10.13
 * Time: 13:10
 * To change this template use File | Settings | File Templates.
 */
public class LogFileEngine extends InMemoryEngine {
    protected File storage;
    protected DataOutputStream logOut;

    protected void loadFromStorage() throws IOException {
        if (!storage.exists()) return;

        FileInputStream in = new FileInputStream(storage);
        DataInputStream log = new DataInputStream(in);
        int size = log.readInt();
        for (int i = 0; i < size; i++) {
            int keyLen = log.readInt();
            byte[] key = new byte[keyLen];
            log.readFully(key);

            int dataLen = log.readInt();
            if (dataLen == -1) {
                super.remove(ByteBuffer.wrap(key));
            } else {
                byte[] data = new byte[dataLen];
                log.readFully(data);
                super.put(ByteBuffer.wrap(key), ByteBuffer.wrap(data));
            }
        }
        long totalBytes = in.getChannel().position();
        log.close(); // also closes 'in'

        FileChannel outChannel = new FileOutputStream(storage, true).getChannel();
        outChannel.truncate(totalBytes);
        outChannel.close();
    }

    protected void checkStorageExistence() throws IOException {
        if (!storage.exists()) {
            storage.createNewFile();
            DataOutputStream log = new DataOutputStream(new FileOutputStream(storage));
            log.writeInt(0);
            log.close();
        }
        if (logOut == null) {
            logOut = new DataOutputStream(new FileOutputStream(storage, true));
        }
    }

    protected void appendToLog(ByteBuffer key, ByteBuffer value) {
        try {
            checkStorageExistence();
            logOut.writeInt(key.limit());
            logOut.write(key.array());

            if (value != null) {
                logOut.writeInt(value.limit());
                logOut.write(value.array());
            } else {
                logOut.writeInt(-1);
            }
            logOut.flush();

            RandomAccessFile out = new RandomAccessFile(storage, "rw");
            out.seek(0);
            int oldSize = out.readInt();
            out.seek(0);
            out.writeInt(oldSize + 1);
            out.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public LogFileEngine(File storage) throws IOException {
        this.storage = storage;
        loadFromStorage();
    }
    public void close() {
        try {
            logOut.close();
        } catch (IOException e) {
        }
    }

    @Override
    public void clear() {
        // This method does not throw an exception even if
        // the log does not exist
        storage.delete();
        super.clear();
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        appendToLog(key, value);
        return super.put(key, value);
    }

    @Override
    public ByteBuffer remove(Object key) {
        appendToLog((ByteBuffer)key, null);
        return super.remove(key);
    }
}

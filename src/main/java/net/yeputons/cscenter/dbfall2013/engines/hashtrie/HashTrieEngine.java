package net.yeputons.cscenter.dbfall2013.engines.hashtrie;

import net.yeputons.cscenter.dbfall2013.engines.FileStorableDbEngine;
import net.yeputons.cscenter.dbfall2013.engines.SimpleEngine;
import net.yeputons.cscenter.dbfall2013.util.HugeMappedFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 05.10.13
 * Time: 20:59
 * To change this template use File | Settings | File Templates.
 */
public class HashTrieEngine extends SimpleEngine implements FileStorableDbEngine, Iterable<Map.Entry<ByteBuffer, ByteBuffer>> {
    final Logger log = LoggerFactory.getLogger(HashTrieEngine.class);

    protected File storage;
    protected RandomAccessFile dataFile;
    protected HugeMappedFile data;
    protected long dataUsedLength;

    protected final MessageDigest md;
    protected final int MD_LEN = 160 / 8;
    protected int currentSize;

    protected Object compactionLock = new Object();
    protected volatile boolean compactionInProgress = false;
    protected volatile Map<ByteBuffer, ByteBuffer> modificationsDuringCompaction;

    protected static final long SIGNATURE_OFFSET = 0;
    protected static final byte[] SIGNATURE = { 'Y', 'D', 'B', 2 };
    protected static final long USED_LENGTH_OFFSET = 4;
    protected static final long ROOT_NODE_OFFSET = 12;

    protected int calcSize(long offset) throws IOException {
        TrieNode node = TrieNode.createFromFile(data, offset);
        if (node instanceof LeafNode) {
            LeafNode leaf = (LeafNode)node;
            return leaf.value == null ? 0 : 1;
        }
        InnerNode inner = (InnerNode)node;
        int res = 0;
        for (int i = 0; i < inner.next.length; i++) {
            long off = inner.next[i];
            if (off != 0)
                res += calcSize(off);
        }
        return res;
    }

    private void resetStorage() throws IOException {
        dataUsedLength = ROOT_NODE_OFFSET;
        dataFile.seek(0);
        dataFile.write(SIGNATURE);
        dataFile.writeLong(dataUsedLength);
        dataFile.getFD().sync();

        data = new HugeMappedFile(dataFile.getChannel());

        long offset = appendToStorage(InnerNode.estimateSize());
        InnerNode.writeToBuffer(data, offset);
    }

    protected void openStorage() throws IOException {
        dataFile = new RandomAccessFile(storage, "rw");
        if (dataFile.length() == 0) {
            resetStorage();
        } else {
            data = new HugeMappedFile(dataFile.getChannel());
            byte[] readSig = new byte[SIGNATURE.length];
            data.get(readSig);
            if (!Arrays.equals(SIGNATURE, readSig))
                throw new IOException("Invalid DB signature");
            dataUsedLength = data.getLong();
        }
        if (dataUsedLength > dataFile.length())
            throw new IOException("Invalid DB: used length is greater than file length");
        currentSize = calcSize(ROOT_NODE_OFFSET);
    }

    public HashTrieEngine(File storage) throws IOException {
        this.storage = storage;
        openStorage();
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
    @Override
    public void flush() throws IOException {
        data.force();
    }
    @Override
    public void close() throws IOException {
        flush();
        data = null;
        dataFile.close();
        dataFile = null;
    }

    public void reopen() throws IOException {
        if (data != null)
            throw new IllegalStateException("Engine should be closed before reopening");
        openStorage();
    }

    @Override
    public void clear() {
        synchronized (compactionLock) {
            if (compactionInProgress)
                throw new IllegalStateException("Compaction is in progress. Please wait and try again");
        }

        try {
            flush();
            resetStorage();
            currentSize = calcSize(ROOT_NODE_OFFSET);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<ByteBuffer> keySet() {
        Set<ByteBuffer> keySet = new HashSet<ByteBuffer>();
        for (Entry<ByteBuffer, ByteBuffer> entry : this.entryIterable())
            keySet.add(entry.getKey());
        return keySet;
    }

    @Override
    public Set<Entry<ByteBuffer, ByteBuffer>> entrySet() {
        Set<Entry<ByteBuffer, ByteBuffer>> entrySet = new HashSet<Entry<ByteBuffer, ByteBuffer>>();
        for (Entry<ByteBuffer, ByteBuffer> entry : this.entryIterable())
            entrySet.add(entry);
        return entrySet;
    }

    public Iterable<Entry<ByteBuffer, ByteBuffer>> entryIterable() {
        return this;
    }

    @Override
    public int size() {
        return currentSize;
    }

    protected LeafNode getNode(ByteBuffer key) {
        byte[] hash = md.digest(key.array());
        assert hash.length == MD_LEN;

        try {
            TrieNode node = TrieNode.createFromFile(data, ROOT_NODE_OFFSET);

            int hashPtr = 0;
            while (!(node instanceof LeafNode)) {
                long ptr = ((InnerNode)node).next[hash[hashPtr] & 0xFF];
                if (ptr == 0) throw new NoSuchElementException();
                node = TrieNode.createFromFile(data, ptr);
                hashPtr++;
            }

            LeafNode leaf = (LeafNode)node;
            if (leaf.value == null)
                throw new NoSuchElementException();

            if (!leaf.key.equals(key)) {
                byte[] hash2 = md.digest(leaf.key.array());
                for (int i = 0; i < hashPtr; i++)
                    assert(hash[i] == hash2[i]);
                if (hashPtr < hash.length) throw new NoSuchElementException();
                assert hash.equals(hash2);
                throw new RuntimeException("SHA-1 collision!");
            }
            return leaf;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer get(Object _key) {
        if (size() == 0) return null;
        ByteBuffer key = (ByteBuffer)_key;

        synchronized (compactionLock) {
            if (compactionInProgress && modificationsDuringCompaction.containsKey(_key)) {
                return modificationsDuringCompaction.get(_key);
            }
        }

        try {
            LeafNode leaf = getNode(key);
            return leaf.value;
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        if (key == null || value == null)
            throw new NullPointerException("key and value should not be nulls");

        synchronized (compactionLock) {
            if (compactionInProgress) {
                ByteBuffer oldValue = null;
                try {
                    oldValue = getNode(key).value;
                } catch (NoSuchElementException e) {
                }
                if (modificationsDuringCompaction.containsKey(key)) {
                    oldValue = modificationsDuringCompaction.get(key);
                }
                if (oldValue == null) currentSize++;
                modificationsDuringCompaction.put(key, value);
                return oldValue;
            }
        }

        byte[] hash = md.digest(key.array());
        assert hash.length == MD_LEN;

        try {
            TrieNode node = TrieNode.createFromFile(data, ROOT_NODE_OFFSET);
            InnerNode parent = null;
            int parentC = -1;
            int hashPtr = 0;
            while (!(node instanceof LeafNode)) {
                InnerNode inner = (InnerNode)node;
                int c = hash[hashPtr] & 0xFF;

                parent = inner;
                parentC = c;
                if (inner.next[c] != 0) {
                    node = TrieNode.createFromFile(data, inner.next[c]);
                } else {
                    long offset = appendToStorage(LeafNode.estimateSize(key, value));
                    LeafNode leaf = LeafNode.writeToBuffer(data, offset, key, value);
                    inner.setNext(c, leaf.offset);
                    node = leaf;
                    currentSize += 1;
                    return null;
                }
                hashPtr++;
            }

            LeafNode leaf = (LeafNode)node;
            if (leaf.key.equals(key)) {
                ByteBuffer oldValue = leaf.value;
                try {
                    leaf.setValue(value);
                } catch (ValueIsBiggerThanOldException e) {
                    long offset = appendToStorage(LeafNode.estimateSize(key, value));
                    LeafNode newLeaf = LeafNode.writeToBuffer(data, offset, key, value);
                    parent.setNext(parentC, newLeaf.offset);
                }
                if (oldValue == null) {
                    currentSize += 1;
                }
                return oldValue;
            } else {
                if (hashPtr >= hash.length)
                    throw new RuntimeException("SHA-1 collision!");

                currentSize += 1;
                if (leaf.value == null) {
                    long offset = appendToStorage(LeafNode.estimateSize(key, value));
                    LeafNode newLeaf = LeafNode.writeToBuffer(data, offset, key, value);
                    parent.setNext(parentC, newLeaf.offset);
                    return null;
                }

                byte[] hash2 = md.digest(leaf.key.array());
                while (true) {
                    long offset = appendToStorage(InnerNode.estimateSize());
                    InnerNode newInner = InnerNode.writeToBuffer(data, offset);
                    parent.setNext(parentC, newInner.offset);

                    parent = newInner;
                    int c1 = hash[hashPtr] & 0xFF, c2 = hash2[hashPtr] & 0xFF;
                    if (c1 != c2) break;

                    parentC = c1;
                    hashPtr++;
                }
                parent.setNext(hash2[hashPtr] & 0xFF, leaf.offset);

                long offset = appendToStorage(LeafNode.estimateSize(key, value));
                LeafNode newLeaf = LeafNode.writeToBuffer(data, offset, key, value);
                parent.setNext(hash[hashPtr] & 0xFF, newLeaf.offset);
                return null;
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private long appendToStorage(long size) throws IOException {
        long res = dataUsedLength;
        while (dataUsedLength + size > dataFile.length()) {
            dataFile.setLength(2 * dataFile.length());
            data = new HugeMappedFile(dataFile.getChannel());
        }
        dataUsedLength += size;
        data.putLong(USED_LENGTH_OFFSET, dataUsedLength);
        return res;
    }

    @Override
    public ByteBuffer remove(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        synchronized (compactionLock) {
            if (compactionInProgress) {
                ByteBuffer oldValue = null;
                try {
                    oldValue = getNode(key).value;
                } catch (NoSuchElementException e) {
                }
                if (modificationsDuringCompaction.containsKey(key)) {
                    oldValue = modificationsDuringCompaction.get(key);
                }
                if (oldValue != null) currentSize--;
                modificationsDuringCompaction.put(key, null);
                return oldValue;
            }
        }

        try {
            LeafNode leaf = getNode(key);
            ByteBuffer oldValue = leaf.value;
            leaf.setValue(null);
            currentSize -= 1;
            return oldValue;
        } catch (IOException e) {
            throw new RuntimeException(e);
        } catch (NoSuchElementException e) {
            return null;
        }
    }

    @Override
    public Iterator<Entry<ByteBuffer, ByteBuffer>> iterator() {
        try {
            return new HashTrieIterator(data);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean isCompactionInProgress() {
        synchronized (compactionLock) {
            return compactionInProgress;
        }
    }
    public void runCompaction() throws IOException {
        synchronized (compactionLock) {
            if (compactionInProgress)
                throw new IllegalStateException("Another compaction is in progress");
            compactionInProgress = true;
            modificationsDuringCompaction = new HashMap<ByteBuffer, ByteBuffer>();
        }

        log.info("Compaction was started.");
        long oldSize = dataUsedLength, oldFileSize = dataFile.length();
        log.debug("Creating temporary storage...");
        File tempStorage = File.createTempFile("compaction", ".trie");
        HashTrieEngine tempEngine = new HashTrieEngine(tempStorage);
        log.debug("Filling temporary storage...");
        {
            // We are unable to use for (a : b) here because
            // we need iterator to be garbage collected to fully close
            // the engine and done the unmapping.
            // Looks like local variables are marked as unused only after
            // method's end. We don't have any way to explicitly remove
            // iterator of foreach, so we use hand-written code here
            // (you can use one more method, too)
            Iterator<Map.Entry<ByteBuffer, ByteBuffer> > it;
            synchronized (this) {
                it = this.iterator();
            }
            for (;;) {
                Map.Entry<ByteBuffer, ByteBuffer> entry;
                try {
                    synchronized (this) {
                        entry = it.next();
                    }
                    tempEngine.put(entry.getKey(), entry.getValue());
                } catch (NoSuchElementException e) {
                    break;
                }
            }
            it = null;
        }
        synchronized (this) {
            log.debug("Applying {} pending modifications...", modificationsDuringCompaction.size());
            for (Entry<ByteBuffer, ByteBuffer> entry : modificationsDuringCompaction.entrySet()) {
                if (entry.getValue() == null)
                    tempEngine.remove(entry.getKey());
                else
                    tempEngine.put(entry.getKey(), entry.getValue());
            }

            log.debug("Closing both old and temporary storages...");
            this.close();
            tempEngine.close();
            System.gc();
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
            }
            log.debug("Overriding current storage...");
            if (!storage.delete()) {
                throw new RuntimeException("Unable to delete old storage");
            }
            if (!tempStorage.renameTo(storage)) {
                throw new RuntimeException("Unable to move new storage over the old");
            }
            log.debug("Reopening compacted storage...");
            this.reopen();

            long newSize = dataUsedLength, newFileSize = dataFile.length();
            log.info("Compaction is finished. Used data: from {} to {}, file size: from {} to {}",
                    oldSize, newSize,
                    oldFileSize, newFileSize
            );

            synchronized (compactionLock) {
                compactionInProgress = false;
                modificationsDuringCompaction = null;
            }
        }
    }
}

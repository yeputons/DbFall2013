package net.yeputons.cscenter.dbfall2013.engines.hashtrie;

import net.yeputons.cscenter.dbfall2013.util.HugeMappedFile;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 24.10.13
 * Time: 21:30
 * To change this template use File | Settings | File Templates.
 */
public class HashTrieIterator implements Iterator<Map.Entry<ByteBuffer, ByteBuffer>> {
    HugeMappedFile data;
    Stack<TrieNode> curv;
    Stack<Integer> curpar;
    LeafNode last;

    protected void findNextLeaf() throws IOException {
        int c = 0;
        if (curv.peek() instanceof LeafNode) {
            curv.pop();
            c = curpar.pop();
        }

        while (!curv.empty() && !(curv.peek() instanceof LeafNode)) {
            InnerNode n = (InnerNode)curv.peek();
            if (c == 256) {
                curv.pop();
                c = curpar.pop();
                continue;
            }
            if (n.next[c] == 0) {
                c++;
                continue;
            }

            assert n.next[c] > 0;
            TrieNode chi = TrieNode.createFromFile(data, n.next[c]);
            curv.push(chi);
            curpar.push(c + 1);
        }
    }
    protected void findNext() throws IOException {
        while (curv.empty() || !(curv.peek() instanceof LeafNode) || !(((LeafNode)curv.peek()).value == null))
            findNextLeaf();
    }

    HashTrieIterator(HugeMappedFile data_) throws IOException {
        data_ = data_;
        curv = new Stack<TrieNode>();
        curv.add(TrieNode.createFromFile(data, HashTrieEngine.ROOT_NODE_OFFSET));

        curpar = new Stack<Integer>();
        curpar.add(-1);

        last = null;
        findNext();
    }

    @Override
    public boolean hasNext() {
        return !curv.empty();
    }

    @Override
    public Map.Entry<ByteBuffer, ByteBuffer> next() {
        if (!hasNext())
            throw new NoSuchElementException();
        try {
            last = (LeafNode)curv.peek();
            assert last instanceof LeafNode;
            findNext();
            return new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(last.key, last.value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }
}

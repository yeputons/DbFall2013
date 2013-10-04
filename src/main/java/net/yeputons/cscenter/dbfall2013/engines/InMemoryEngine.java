package net.yeputons.cscenter.dbfall2013.engines;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 27.09.13
 * Time: 20:30
 * To change this template use File | Settings | File Templates.
 */
public class InMemoryEngine extends SimpleEngine {
    Map<ByteBuffer, ByteBuffer> data;

    public InMemoryEngine() {
        data = new HashMap<ByteBuffer, ByteBuffer>();
   }

    @Override
    public int size() {
        return data.size();
    }

    @Override
    public ByteBuffer get(Object key) {
        return data.get(key);
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        if (key == null || value == null)
            throw new NullPointerException("key and value should not be nulls");
        return data.put(key, value);
    }

    @Override
    public ByteBuffer remove(Object key) {
        return data.remove(key);
    }

    @Override
    public void clear() {
        data.clear();
    }

    @Override
    public Set<ByteBuffer> keySet() {
        return data.keySet();
    }
}

package net.yeputons.cscenter.dbfall2013.engines;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 27.09.13
 * Time: 20:32
 * To change this template use File | Settings | File Templates.
 */
public abstract class SimpleEngine implements DbEngine {
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    public boolean containsKey(Object key) {
        return get(key) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        for (Entry<ByteBuffer, ByteBuffer> entry : entrySet())
            if (entry.getValue().equals(value)) return true;
        return false;
    }

    @Override
    public void putAll(Map<? extends ByteBuffer, ? extends ByteBuffer> m) {
        for (Entry<? extends ByteBuffer, ? extends ByteBuffer> item : m.entrySet())
            put(item.getKey(), item.getValue());
    }

    @Override
    public Collection<ByteBuffer> values() {
        Collection<ByteBuffer> result = new LinkedList<ByteBuffer>();
        for (ByteBuffer key : keySet())
            result.add(get(key));
        return result;
    }

    @Override
    public Set<Entry<ByteBuffer, ByteBuffer>> entrySet() {
        Set<Entry<ByteBuffer, ByteBuffer> > result = new HashSet<Entry<ByteBuffer, ByteBuffer> >();
        for (ByteBuffer key : keySet())
            result.add(new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(key, get(key)));
        return result;
    }

    @Override
    public int hashCode() {
        return entrySet().hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        if (o == null) return false;
        if (!(o instanceof Map)) return false;
        return entrySet().equals(((Map) o).entrySet());
    }
}

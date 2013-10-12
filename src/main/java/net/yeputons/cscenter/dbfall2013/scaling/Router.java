package net.yeputons.cscenter.dbfall2013.scaling;

import net.yeputons.cscenter.dbfall2013.engines.SimpleEngine;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.nio.ByteBuffer;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:50
 * To change this template use File | Settings | File Templates.
 */
public class Router extends SimpleEngine {
    ShardingConfiguration conf;

    Router(ShardingConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public int size() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuffer get(Object key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuffer remove(Object key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<ByteBuffer> keySet() {
        throw new UnsupportedOperationException();
    }
}

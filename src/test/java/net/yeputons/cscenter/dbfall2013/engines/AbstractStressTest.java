package net.yeputons.cscenter.dbfall2013.engines;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 17.10.13
 * Time: 12:53
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractStressTest {
    protected ByteBuffer genByteBuffer(Random rnd) {
        int len = 1 + rnd.nextInt(9);
        byte[] res = new byte[len];
        rnd.nextBytes(res);
        return ByteBuffer.wrap(res);
    }

    protected void performRandomOperation(Random rnd, DbEngine engine, Map<ByteBuffer, ByteBuffer> real) {
        int operation = rnd.nextInt(100);
        if (operation < 50) {
            ByteBuffer key = genByteBuffer(rnd);
            ByteBuffer value = genByteBuffer(rnd);
            engine.put(key, value);
            real.put(key, value);
        } else if (operation < 75 && real.size() > 0) {
            Set<ByteBuffer> keys = real.keySet();
            int id = rnd.nextInt(keys.size());
            for (ByteBuffer key : keys)
                if (id-- == 0) {
                    engine.remove(key);
                    real.remove(key);
                    break;
                }
        } else if (operation < 100) {
            ByteBuffer key = genByteBuffer(rnd);
            engine.remove(key);
            real.remove(key);
        }
    }
}

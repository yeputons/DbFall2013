package net.yeputons.cscenter.dbfall2013.engines;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 05.10.13
 * Time: 22:43
 * To change this template use File | Settings | File Templates.
 */
public abstract class FileStorableDbEngineTest {
    protected File storage;

    public FileStorableDbEngineTest() throws IOException {
        storage = File.createTempFile("storage", ".bin");
        storage.delete();
    }

    protected abstract FileStorableDbEngine createEngine() throws IOException;

    private ByteBuffer genByteBuffer(Random rnd) {
        int len = 1 + rnd.nextInt(9);
        byte[] res = new byte[len];
        rnd.nextBytes(res);
        return ByteBuffer.wrap(res);
    }

    @Test
    public void stressTest() throws IOException {
        FileStorableDbEngine engine = createEngine();
        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();
        assertEquals(real, engine);

        Random rnd = new Random();
        for (int step = 0; step < 200; step++) {
            if (rnd.nextInt(100) <= 30) {
                engine.close();
                engine = createEngine();
                assertEquals(real, engine);
            }

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

            assertEquals(real, engine);
        }
    }
}
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
public abstract class FileStorableDbEngineTest extends AbstractStressTest {
    protected File storage;

    public FileStorableDbEngineTest() throws IOException {
        storage = File.createTempFile("storage", ".bin");
        storage.delete();
    }

    protected abstract FileStorableDbEngine createEngine() throws IOException;

    @Test
    public void stressTest() throws IOException {
        FileStorableDbEngine engine = createEngine();
        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();
        assertEquals(real, engine);

        Random rnd = new Random();
        for (int step = 0; step < 500; step++) {
            if (rnd.nextInt(100) <= 30) {
                engine.close();
                engine = createEngine();
                assertEquals(real, engine);
            }
            performRandomOperation(rnd, engine, real);

            assertEquals(engine.size(), engine.keySet().size());
            assertEquals(engine.size(), engine.entrySet().size());
            assertEquals(real, engine);
        }
    }
}
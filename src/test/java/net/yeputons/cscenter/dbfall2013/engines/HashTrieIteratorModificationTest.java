package net.yeputons.cscenter.dbfall2013.engines;

import net.yeputons.cscenter.dbfall2013.engines.hashtrie.HashTrieEngine;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 25.10.13
 * Time: 19:10
 * To change this template use File | Settings | File Templates.
 */
public class HashTrieIteratorModificationTest extends AbstractStressTest {
    final Logger log = LoggerFactory.getLogger(HashTrieIteratorModificationTest.class);
    protected File storage;

    public HashTrieIteratorModificationTest() throws IOException {
        storage = File.createTempFile("storage", ".bin");
        storage.delete();
    }

    @Test
    public void stressTest() throws IOException, InterruptedException {
        final HashTrieEngine engine = new HashTrieEngine(this.storage);
        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();
        assertEquals(real, engine);

        Random rnd = new Random();
        Iterator<Map.Entry<ByteBuffer, ByteBuffer>> it = engine.iterator();

        for (int step = 0; step < 500; step++) {
            performRandomOperation(rnd, engine, real);

            if (!it.hasNext()) it = engine.iterator();
            if (engine.size() > 0) {
                assertEquals(true, it.hasNext());
                Map.Entry<ByteBuffer, ByteBuffer> entry = it.next();
                assertEquals(real.get(entry.getKey()), entry.getValue());
            } else {
                assertEquals(false, it.hasNext());
            }

            assertEquals(engine.size(), engine.keySet().size());
            assertEquals(engine.size(), engine.entrySet().size());
            assertEquals(real.keySet(), engine.keySet());
            assertEquals(real.entrySet(), engine.entrySet());
            assertEquals(real, engine);
        }
    }
}

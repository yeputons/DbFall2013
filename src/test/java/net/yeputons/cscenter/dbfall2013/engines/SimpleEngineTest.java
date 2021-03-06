package net.yeputons.cscenter.dbfall2013.engines;

import net.yeputons.cscenter.dbfall2013.engines.hashtrie.HashTrieEngine;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 01.10.13
 * Time: 13:07
 * To change this template use File | Settings | File Templates.
 */
@RunWith(value = Parameterized.class)
public class SimpleEngineTest {
    SimpleEngine engine;

    protected ByteBuffer str2Buf(String s) {
        return ByteBuffer.wrap(s.getBytes());
    }

    public SimpleEngineTest(SimpleEngine engineToTest) {
        this.engine = engineToTest;
    }

    @Parameterized.Parameters
    public static java.util.Collection<Object[]> data() throws IOException {
        File storage1 = File.createTempFile("test-storage", ".log");
        File storage2 = File.createTempFile("test-storage", ".log");
        storage1.delete();
        storage2.delete();
        Object[][] data = new Object[][] {
                { new InMemoryEngine() },
                { new LogFileEngine(storage1) },
                { new HashTrieEngine(storage2) }
        };
        return Arrays.asList(data);
    }

    protected void checkSize(int expectedSize) {
        assertEquals(expectedSize, engine.size());
        assertEquals(expectedSize == 0, engine.isEmpty());
        assertEquals(engine.size(), engine.keySet().size());
        assertEquals(engine.size(), engine.entrySet().size());
    }

    @Test(expected = NullPointerException.class)
    public void putNullKeyTest() {
        engine.put(null, str2Buf("value"));
    }

    @Test(expected = NullPointerException.class)
    public void putNullValueTest() {
        engine.put(str2Buf("key"), null);
    }

    @Test(expected = NullPointerException.class)
    public void putNullBothTest() {
        engine.put(null, null);
    }

    @Test
    public void equalsTest() {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer, ByteBuffer>();
        Map<ByteBuffer, ByteBuffer> map2 = new HashMap<ByteBuffer, ByteBuffer>();

        map2.put(str2Buf("a"), str2Buf("b"));

        assertEquals(map, engine);
        assertNotSame(map2, engine);

        engine.put(str2Buf("a"), str2Buf("a"));
        assertNotSame(map, engine);
        assertNotSame(map2, engine);

        engine.put(str2Buf("a"), str2Buf("b"));
        assertNotSame(map, engine);
        assertEquals(map2, engine);

        engine.remove(str2Buf("a"));
        assertEquals(map, engine);
        assertNotSame(map2, engine);
    }

    @Test
    public void clearTest() {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer, ByteBuffer>();
        Map<ByteBuffer, ByteBuffer> map2 = new HashMap<ByteBuffer, ByteBuffer>();

        map2.put(str2Buf("a"), str2Buf("b"));

        assertEquals(map, engine);
        assertNotSame(map2, engine);

        engine.put(str2Buf("a"), str2Buf("a"));
        assertNotSame(map, engine);
        assertNotSame(map2, engine);

        engine.clear();
        assertEquals(map, engine);
        assertNotSame(map2, engine);
    }

    @Test
    public void basicTest() {
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("ab")));
        checkSize(0);

        assertEquals(null, engine.put(str2Buf("a"), str2Buf("test")));
        assertEquals(str2Buf("test"), engine.get(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("ab")));
        checkSize(1);

        assertEquals(null, engine.put(str2Buf("ab"), str2Buf("test0")));
        assertEquals(str2Buf("test"), engine.get(str2Buf("a")));
        assertEquals(str2Buf("test0"), engine.get(str2Buf("ab")));
        checkSize(2);

        assertEquals(str2Buf("test"), engine.put(str2Buf("a"), str2Buf("test2")));
        assertEquals(str2Buf("test2"), engine.get(str2Buf("a")));
        assertEquals(str2Buf("test0"), engine.get(str2Buf("ab")));
        checkSize(2);

        assertEquals(str2Buf("test2"), engine.remove(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(str2Buf("test0"), engine.get(str2Buf("ab")));
        checkSize(1);

        assertEquals(str2Buf("test0"), engine.remove(str2Buf("ab")));
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("ab")));
        checkSize(0);

        assertEquals(null, engine.remove(str2Buf("ab")));
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("ab")));
        checkSize(0);

        assertEquals(null, engine.put(str2Buf("ab"), str2Buf("test3")));
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(str2Buf("test3"), engine.get(str2Buf("ab")));
        checkSize(1);

        assertEquals(str2Buf("test3"), engine.remove(str2Buf("ab")));
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("ab")));
        checkSize(0);

        assertEquals(null, engine.remove(str2Buf("ab")));
        assertEquals(null, engine.get(str2Buf("a")));
        assertEquals(null, engine.get(str2Buf("ab")));
        checkSize(0);
    }
}

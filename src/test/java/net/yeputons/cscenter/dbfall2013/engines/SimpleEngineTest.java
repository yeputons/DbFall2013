package net.yeputons.cscenter.dbfall2013.engines;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

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
    public static java.util.Collection<Object[]> data() {
        Object[][] data = new Object[][] {
                { new InMemoryEngine() }
        };
        return Arrays.asList(data);
    }

    protected void checkSize(int expectedSize) {
        assertEquals(expectedSize, engine.size());
        assertEquals(expectedSize == 0, engine.isEmpty());
        assertEquals(engine.size(), engine.keySet().size());
        assertEquals(engine.size(), engine.entrySet().size());
    }

    @Test
    public void equalsTest() {
        Map<ByteBuffer, ByteBuffer> map = new HashMap<ByteBuffer, ByteBuffer>();
        Map<ByteBuffer, ByteBuffer> map2 = new HashMap<ByteBuffer, ByteBuffer>();

        map2.put(str2Buf("a"), str2Buf("b"));

        assertEquals(engine, map);
        assertNotSame(engine, map2);

        engine.put(str2Buf("a"), str2Buf("a"));
        assertNotSame(engine, map);
        assertNotSame(engine, map2);

        engine.put(str2Buf("a"), str2Buf("b"));
        assertNotSame(engine, map);
        assertEquals(engine, map2);

        engine.remove(str2Buf("a"));
        assertEquals(engine, map);
        assertNotSame(engine, map2);
    }

    @Test
    public void basicTest() {
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), null);
        checkSize(0);

        assertEquals(engine.put(str2Buf("a"), str2Buf("test")), null);
        assertEquals(engine.get(str2Buf("a")), str2Buf("test"));
        assertEquals(engine.get(str2Buf("ab")), null);
        checkSize(1);

        assertEquals(engine.put(str2Buf("ab"), str2Buf("test0")), null);
        assertEquals(engine.get(str2Buf("a")), str2Buf("test"));
        assertEquals(engine.get(str2Buf("ab")), str2Buf("test0"));
        checkSize(2);

        assertEquals(engine.put(str2Buf("a"), str2Buf("test2")), str2Buf("test"));
        assertEquals(engine.get(str2Buf("a")), str2Buf("test2"));
        assertEquals(engine.get(str2Buf("ab")), str2Buf("test0"));
        checkSize(2);

        engine.remove(str2Buf("a"));
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), str2Buf("test0"));
        checkSize(1);

        assertEquals(engine.remove(str2Buf("ab")), str2Buf("test0"));
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), null);
        checkSize(0);

        assertEquals(engine.remove(str2Buf("ab")), null);
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), null);
        checkSize(0);
    }
}

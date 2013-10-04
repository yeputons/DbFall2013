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
    }
}

package net.yeputons.cscenter.dbfall2013.engines;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

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

    @Test
    public void basicTest() {
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), null);
        assertEquals(engine.size(), 0);
        assertEquals(engine.isEmpty(), true);
        assertEquals(engine.keySet().size(), engine.size());

        engine.put(str2Buf("a"), str2Buf("test"));
        assertEquals(engine.get(str2Buf("a")), str2Buf("test"));
        assertEquals(engine.get(str2Buf("ab")), null);
        assertEquals(engine.size(), 1);
        assertEquals(engine.isEmpty(), false);
        assertEquals(engine.keySet().size(), engine.size());

        engine.put(str2Buf("ab"), str2Buf("test0"));
        assertEquals(engine.get(str2Buf("a")), str2Buf("test"));
        assertEquals(engine.get(str2Buf("ab")), str2Buf("test0"));
        assertEquals(engine.size(), 2);
        assertEquals(engine.isEmpty(), false);
        assertEquals(engine.keySet().size(), engine.size());

        engine.put(str2Buf("a"), str2Buf("test2"));
        assertEquals(engine.get(str2Buf("a")), str2Buf("test2"));
        assertEquals(engine.get(str2Buf("ab")), str2Buf("test0"));
        assertEquals(engine.size(), 2);
        assertEquals(engine.isEmpty(), false);
        assertEquals(engine.keySet().size(), engine.size());

        engine.remove(str2Buf("a"));
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), str2Buf("test0"));
        assertEquals(engine.size(), 1);
        assertEquals(engine.isEmpty(), false);
        assertEquals(engine.keySet().size(), engine.size());

        engine.remove(str2Buf("ab"));
        assertEquals(engine.get(str2Buf("a")), null);
        assertEquals(engine.get(str2Buf("ab")), null);
        assertEquals(engine.size(), 0);
        assertEquals(engine.isEmpty(), true);
        assertEquals(engine.keySet().size(), engine.size());
    }
}

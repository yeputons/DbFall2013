package net.yeputons.cscenter.dbfall2013.engines;

import net.yeputons.cscenter.dbfall2013.scaling.Router;
import net.yeputons.cscenter.dbfall2013.scaling.ShardDescription;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingConfiguration;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;

import static org.junit.Assert.assertEquals;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 13.10.13
 * Time: 0:50
 * To change this template use File | Settings | File Templates.
 */
public class ShardingTest {
    List<ShardingNode> nodes;
    ShardingConfiguration configuration;
    Router router;

    @Before
    public void setUp() throws Exception {
        configuration = new ShardingConfiguration();
        nodes = new ArrayList<ShardingNode>();
        int port = ShardingConfiguration.DEFAULT_PORT;
        for (int start = 0; start < 256; start += 256) {
            ShardDescription item = new ShardDescription();
            item.host = "localhost";
            item.port = port;
            String startHash = String.format("%02x", start);
            configuration.shards.put(startHash, item);

            final ShardingNode node = new ShardingNode();
            final int port_ = port;
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        node.run(File.createTempFile("sharding", ".trie"), InetAddress.getByName("localhost"), port_);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
            nodes.add(node);
            th.start();
            port++;
        }
    }

    @After
    public void tearDown() {
        for (ShardingNode node : nodes)
            node.stop();
    }

    private ByteBuffer genByteBuffer(Random rnd) {
        int len = 1 + rnd.nextInt(9);
        byte[] res = new byte[len];
        rnd.nextBytes(res);
        return ByteBuffer.wrap(res);
    }

    @Test
    public void stressTest() throws IOException {
        Router engine = new Router(configuration);
        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();

        Random rnd = new Random();
        for (int step = 0; step < 20; step++) {
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

            for (Map.Entry<ByteBuffer, ByteBuffer> entry : real.entrySet()) {
                assertEquals(entry.getValue(), engine.get(entry.getKey()));
            }
        }
    }
}

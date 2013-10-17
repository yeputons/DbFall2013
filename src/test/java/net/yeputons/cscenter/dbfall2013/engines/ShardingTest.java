package net.yeputons.cscenter.dbfall2013.engines;

import net.yeputons.cscenter.dbfall2013.scaling.Router;
import net.yeputons.cscenter.dbfall2013.scaling.ShardDescription;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingConfiguration;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
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
public class ShardingTest extends AbstractStressTest {
    List<ShardingNode> nodes;
    List<Thread> nodeThreads;
    ShardingConfiguration configuration;
    Router router;

    volatile boolean threadFailed;

    @Before
    public void setUp() throws Exception {
        threadFailed = false;

        configuration = new ShardingConfiguration();
        nodes = new ArrayList<ShardingNode>();
        nodeThreads = new ArrayList<Thread>();
        int port = ShardingConfiguration.DEFAULT_PORT;
        for (int start = 0; start < 256; start += 64) {
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
                        threadFailed = true;
                    }
                }
            });
            nodes.add(node);
            nodeThreads.add(th);
            th.start();
            port++;
        }
    }

    @After
    public void tearDown() throws InterruptedException {
        for (ShardingNode node : nodes)
            node.stop();
        for (Thread th : nodeThreads)
            th.join();
        if (threadFailed)
            throw new RuntimeException("Got errors in some of the node threads");
    }

    @Test
    public void stressTest() throws IOException {
        Router engine = new Router(configuration);
        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();

        Random rnd = new Random();
        // 350 iterations only because we need to check equality
        // after each iteration (hence, quadratic complexity appears)
        for (int step = 0; step < 350; step++) {
            performRandomOperation(rnd, engine, real);
            assertEquals(real.keySet(), engine.keySet());
            assertEquals(real, engine);
        }
    }

    @Test
    public void multithreadedStressTest() throws IOException, InterruptedException {
        List<Thread> ths = new ArrayList<Thread>();
        for (int i = 0; i < 10; i++) {
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Router engine = new Router(configuration);
                        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();
                        Random rnd = new Random();
                        // Here we don't check anything, so 4000 iterations will run in linear time
                        for (int step = 0; step < 4000; step++)
                            performRandomOperation(rnd, engine, real);
                    } catch (Exception e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        threadFailed = true;
                    }
                }
            });
            th.start();
            ths.add(th);
        }
        for (Thread th : ths)
            th.join();
        if (threadFailed)
            throw new RuntimeException("Got errors in some of the threads");
    }
}

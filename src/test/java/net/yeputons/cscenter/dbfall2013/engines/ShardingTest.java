package net.yeputons.cscenter.dbfall2013.engines;

import net.yeputons.cscenter.dbfall2013.scaling.Router;
import net.yeputons.cscenter.dbfall2013.scaling.ShardDescription;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingConfiguration;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    static final Logger log = LoggerFactory.getLogger(ShardingTest.class);

    List<ShardingNode> nodes;
    List<Thread> nodeThreads;
    ShardingConfiguration configuration;
    Router router;

    volatile boolean threadFailed;

    @Before
    public void setUp() throws Exception {
        threadFailed = false;

        log.info("Starting shards...");
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
            final int start_ = start;
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("Shard for {} is starting on port {}", start_, port_);
                    try {
                        node.run(File.createTempFile("sharding", ".trie"), InetAddress.getByName("localhost"), port_);
                    } catch (Exception e) {
                        log.error("Exception is caught in node thread", e);
                        threadFailed = true;
                    }
                }
            });
            nodes.add(node);
            nodeThreads.add(th);
            th.start();
            port++;
        }
        log.info("Shards were started.");
    }

    @After
    public void tearDown() throws InterruptedException {
        log.info("Stopping shards...");
        for (ShardingNode node : nodes)
            node.stop();
        for (Thread th : nodeThreads)
            th.join();
        if (threadFailed)
            throw new RuntimeException("Got errors in some of the threads");
        log.info("Shards have been stopped");
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
            final int id = i;
            Thread th = new Thread(new Runnable() {
                @Override
                public void run() {
                    log.info("Stress thread is running", id);
                    try {
                        Router engine = new Router(configuration);
                        Map<ByteBuffer, ByteBuffer> real = new HashMap<ByteBuffer, ByteBuffer>();
                        Random rnd = new Random();
                        // Here we don't check anything, so 4000 iterations will run in linear time
                        for (int step = 0; step < 4000; step++)
                            performRandomOperation(rnd, engine, real);
                    } catch (Exception e) {
                        log.error("Exception is caught in stress thread", e);
                        threadFailed = true;
                    }
                    log.info("Stress thread has done its work");
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

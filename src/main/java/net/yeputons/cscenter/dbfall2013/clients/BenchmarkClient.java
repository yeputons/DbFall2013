package net.yeputons.cscenter.dbfall2013.clients;

import net.yeputons.cscenter.dbfall2013.engines.FileStorableDbEngine;
import net.yeputons.cscenter.dbfall2013.engines.HashTrieEngine;
import net.yeputons.cscenter.dbfall2013.engines.LogFileEngine;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 05.10.13
 * Time: 19:50
 * To change this template use File | Settings | File Templates.
 */
public class BenchmarkClient {
    public static void main(String[] args) throws Exception {
        File f = File.createTempFile("benchmark", ".log");
        f.delete();

        final FileStorableDbEngine engine = new HashTrieEngine(f);

        long startTime = System.currentTimeMillis();
        byte[] key = new byte[10];
        byte[] value = new byte[10];
        Random rand = new Random();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    engine.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }));
        for (int step = 1;; step++) {
            rand.nextBytes(key);
            rand.nextBytes(value);
            engine.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
            if (step % 300 == 0) {
                engine.flush();
                double elapsed = (double)(System.currentTimeMillis() - startTime) / 1000.0;
                System.out.printf("size=%d, elapsed=%.2f, puts per second=%.2f\n", engine.size(), elapsed, (double)engine.size() / elapsed);
            }
        }
    }
}

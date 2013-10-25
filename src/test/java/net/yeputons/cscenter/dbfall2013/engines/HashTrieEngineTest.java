package net.yeputons.cscenter.dbfall2013.engines;

import net.yeputons.cscenter.dbfall2013.engines.hashtrie.HashTrieEngine;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 05.10.13
 * Time: 22:52
 * To change this template use File | Settings | File Templates.
 */
public class HashTrieEngineTest extends FileStorableDbEngineTest {
    public HashTrieEngineTest() throws IOException {
        super();
    }

    @Override
    protected FileStorableDbEngine createEngine() throws IOException {
        return new HashTrieEngine(this.storage);
    }

    @Override
    protected void performRandomReload(Random rnd, FileStorableDbEngine engine_) throws IOException {
        HashTrieEngine engine = (HashTrieEngine) engine_;
        if (rnd.nextInt(100) <= 1) {
            engine.runCompaction();
        }
    }
}

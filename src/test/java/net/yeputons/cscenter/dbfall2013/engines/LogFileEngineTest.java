package net.yeputons.cscenter.dbfall2013.engines;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 05.10.13
 * Time: 22:51
 * To change this template use File | Settings | File Templates.
 */
public class LogFileEngineTest extends FileStorableDbEngineTest {
    public LogFileEngineTest() throws IOException {
        super();
    }

    @Override
    protected FileStorableDbEngine createEngine() throws IOException {
        return new LogFileEngine(this.storage);
    }
}

package net.yeputons.cscenter.dbfall2013.engines;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 05.10.13
 * Time: 22:40
 * To change this template use File | Settings | File Templates.
 */
public interface FileStorableDbEngine extends DbEngine {
    public void flush() throws IOException;
    public void close() throws IOException;
}

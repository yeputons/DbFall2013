package net.yeputons.cscenter.dbfall2013.scaling;

import java.io.IOException;
import java.net.Socket;

public class ShardDescription {
    public String host;
    public int port;

    public Socket openSocket() throws IOException {
        return new Socket(host, port);
    }
}
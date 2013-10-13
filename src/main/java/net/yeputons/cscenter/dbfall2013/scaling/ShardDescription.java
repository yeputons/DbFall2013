package net.yeputons.cscenter.dbfall2013.scaling;

import java.io.IOException;
import java.net.Socket;

public class ShardDescription {
    public String host;
    public int port;

    public Socket openSocket() throws IOException {
        return new Socket(host, port);
    }

    @Override
    public int hashCode() {
        return host.hashCode() * 239017 + port;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof ShardDescription))
            return false;
        ShardDescription desc = (ShardDescription)obj;
        return host.equals(desc.host) && port == desc.port;
    }
}
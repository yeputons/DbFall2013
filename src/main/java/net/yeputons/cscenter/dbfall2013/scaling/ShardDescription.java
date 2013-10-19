package net.yeputons.cscenter.dbfall2013.scaling;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

public class ShardDescription {
    public InetSocketAddress address;

    public Socket openSocket() throws IOException {
        return new Socket(address.getAddress(), address.getPort());
    }

    @Override
    public int hashCode() {
        return address.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof ShardDescription))
            return false;
        ShardDescription desc = (ShardDescription)obj;
        return address.equals(desc.address);
    }
}
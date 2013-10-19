package net.yeputons.cscenter.dbfall2013.scaling;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class ShardDescription {
    protected static Random random = new Random();
    public List<InetSocketAddress> replicas;

    public Socket openRwSocket() throws IOException {
        return new Socket(replicas.get(0).getAddress(), replicas.get(0).getPort());
    }
    public Socket openReadSocket() throws IOException {
        int id = random.nextInt(replicas.size());
        return new Socket(replicas.get(id).getAddress(), replicas.get(id).getPort());
    }

    public ShardDescription() {
        replicas = new ArrayList<InetSocketAddress>();
    }

    @Override
    public int hashCode() {
        return replicas.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (obj == this) return true;
        if (!(obj instanceof ShardDescription))
            return false;
        ShardDescription desc = (ShardDescription)obj;
        return replicas.equals(desc.replicas);
    }
}
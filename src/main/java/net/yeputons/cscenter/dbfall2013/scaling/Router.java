package net.yeputons.cscenter.dbfall2013.scaling;

import net.yeputons.cscenter.dbfall2013.engines.SimpleEngine;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:50
 * To change this template use File | Settings | File Templates.
 */
public class Router extends SimpleEngine implements AutoCloseable {
    protected class ShardConnection {
        public Socket s;
        public DataInputStream in;
        public DataOutputStream out;

        public ShardConnection(Socket s) throws IOException {
            this.s = s;
            in = new DataInputStream(s.getInputStream());
            out = new DataOutputStream(s.getOutputStream());
        }
        public void readOk() throws IOException {
            byte[] res = new byte[2];
            in.readFully(res);
            if (!Arrays.equals(res, "ok".getBytes())) {
                throw new RuntimeException("Server returned an unknown status: " + new String(res));
            }
        }
    }

    ShardingConfiguration conf;
    HashMap<ShardDescription, ShardConnection> connections;

    public Router(ShardingConfiguration conf) {
        this.conf = conf;
        connections = new HashMap<ShardDescription, ShardConnection>();
    }

    protected ShardConnection getConnection(ShardDescription shard) throws IOException {
        ShardConnection conn = connections.get(shard);
        if (conn == null) {
            conn = new ShardConnection(shard.openSocket());
            connections.put(shard, conn);
        }
        return conn;
    }
    protected ShardConnection getConnection(byte[] key) throws IOException {
        return getConnection(conf.getShard(key));
    }

    @Override
    public ByteBuffer get(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        try {
            ShardConnection c = getConnection(key.array());
            c.out.write("get".getBytes());
            c.out.writeInt(key.limit());
            c.out.write(key.array());

            c.readOk();
            int len = c.in.readInt();
            if (len == -1) return null;

            byte[] value = new byte[len];
            c.in.readFully(value);

            return ByteBuffer.wrap(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        try {
            ShardConnection c = getConnection(key.array());
            c.out.write("put".getBytes());
            c.out.writeInt(value.limit());
            c.out.write(value.array());
            c.out.writeInt(key.limit());
            c.out.write(key.array());
            c.readOk();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuffer remove(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        try {
            ShardConnection c = getConnection(key.array());
            c.out.write("del".getBytes());
            c.out.writeInt(key.limit());
            c.out.write(key.array());
            c.readOk();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
        for (Entry<String, ShardDescription> shard : conf.shards.entrySet()) {
            Socket s = null;
            try {
                ShardConnection c = getConnection(shard.getValue());
                c.out.write("clr".getBytes());
                c.readOk();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<ByteBuffer> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws Exception {
        for (Entry<ShardDescription, ShardConnection> c : connections.entrySet()) {
            c.getValue().in.close();
            c.getValue().out.close();
            c.getValue().s.close();
        }
        connections.clear();
    }
}

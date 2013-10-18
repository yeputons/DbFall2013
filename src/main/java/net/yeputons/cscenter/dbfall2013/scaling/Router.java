package net.yeputons.cscenter.dbfall2013.scaling;

import net.yeputons.cscenter.dbfall2013.engines.SimpleEngine;
import net.yeputons.cscenter.dbfall2013.util.DataInputStream;
import net.yeputons.cscenter.dbfall2013.util.DataOutputStream;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;

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
                throw new RouterCommunicationException("Server returned an unknown status: " + new String(res));
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
    protected void closeConnection(ShardDescription shard) {
        ShardConnection conn = connections.get(shard);
        if (conn != null) {
            try {
                conn.in.close();
                conn.out.close();
                conn.s.close();
            } catch (IOException e) {
            }
            connections.remove(shard);
        }
    }
    protected void closeConnection(byte[] key) {
        closeConnection(conf.getShard(key));
    }

    @Override
    public ByteBuffer get(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        try {
            ShardConnection c = getConnection(key.array());
            c.out.write("get".getBytes());
            c.out.writeArray(key.array());

            c.readOk();

            byte[] value = c.in.readArray();
            return value == null ? null : ByteBuffer.wrap(value);
        } catch (RouterCommunicationException | IOException e) {
            closeConnection(key.array());
            throw new RouterCommunicationException(e);
        }
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        try {
            ShardConnection c = getConnection(key.array());
            c.out.write("put".getBytes());
            c.out.writeArray(value.array());
            c.out.writeArray(key.array());
            c.readOk();
        } catch (RouterCommunicationException | IOException e) {
            closeConnection(key.array());
            throw new RouterCommunicationException(e);
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuffer remove(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        try {
            ShardConnection c = getConnection(key.array());
            c.out.write("del".getBytes());
            c.out.writeArray(key.array());
            c.readOk();
        } catch (IOException e) {
            closeConnection(key.array());
            throw new RouterCommunicationException(e);
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
            } catch (RouterCommunicationException | IOException e) {
                closeConnection(shard.getValue());
                throw new RouterCommunicationException(e);
            }
        }
    }

    @Override
    public int size() {
        int sum = 0;
        for (Entry<String, ShardDescription> shard : conf.shards.entrySet()) {
            try {
                ShardConnection c = getConnection(shard.getValue());
                c.out.write("siz".getBytes());
                c.readOk();
                int res = c.in.readInt();
                sum += res;
            } catch (RouterCommunicationException | IOException e) {
                closeConnection(shard.getValue());
                throw new RouterCommunicationException(e);
            }
        }
        return sum;
    }

    @Override
    public Set<ByteBuffer> keySet() {
        Set<ByteBuffer> res = new HashSet<ByteBuffer>();
        for (Entry<String, ShardDescription> shard : conf.shards.entrySet()) {
            try {
                ShardConnection c = getConnection(shard.getValue());
                c.out.write("key".getBytes());
                c.readOk();
                int count = c.in.readInt();
                while (count --> 0) {
                    res.add(ByteBuffer.wrap(c.in.readArray()));
                }
            } catch (RouterCommunicationException | IOException e) {
                closeConnection(shard.getValue());
                throw new RouterCommunicationException(e);
            }
        }
        return res;
    }

    @Override
    public Set<Entry<ByteBuffer, ByteBuffer>> entrySet() {
        Set<Entry<ByteBuffer, ByteBuffer>> res = new HashSet<Entry<ByteBuffer, ByteBuffer>>();
        for (Entry<String, ShardDescription> shard : conf.shards.entrySet()) {
            try {
                ShardConnection c = getConnection(shard.getValue());
                c.out.write("its".getBytes());
                c.readOk();
                int count = c.in.readInt();
                while (count --> 0) {
                    byte[] key = c.in.readArray();
                    byte[] value = c.in.readArray();
                    res.add(new AbstractMap.SimpleEntry<ByteBuffer, ByteBuffer>(
                            ByteBuffer.wrap(key),
                            ByteBuffer.wrap(value)
                    ));
                }
            } catch (RouterCommunicationException | IOException e) {
                closeConnection(shard.getValue());
                throw new RouterCommunicationException(e);
            }
        }
        return res;
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

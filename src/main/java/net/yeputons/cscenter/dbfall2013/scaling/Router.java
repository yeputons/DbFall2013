package net.yeputons.cscenter.dbfall2013.scaling;

import net.yeputons.cscenter.dbfall2013.engines.SimpleEngine;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:50
 * To change this template use File | Settings | File Templates.
 */
public class Router extends SimpleEngine {
    ShardingConfiguration conf;

    public Router(ShardingConfiguration conf) {
        this.conf = conf;
    }

    @Override
    public ByteBuffer get(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        try {
            Socket s = conf.getShard(key.array()).openSocket();
            DataInputStream in = new DataInputStream(s.getInputStream());
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.write("get".getBytes());
            out.writeInt(key.limit());
            out.write(key.array());

            byte[] res = new byte[2];
            in.readFully(res);
            if (!Arrays.equals(res, "ok".getBytes())) {
                throw new RuntimeException("Server returned an unknown status: " + new String(res));
            }
            int len = in.readInt();
            byte[] value = new byte[len];
            in.readFully(value);
            s.close();

            return ByteBuffer.wrap(value);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ByteBuffer put(ByteBuffer key, ByteBuffer value) {
        try {
            Socket s = conf.getShard(key.array()).openSocket();
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.write("put".getBytes());
            out.writeInt(value.limit());
            out.write(value.array());
            out.writeInt(key.limit());
            out.write(key.array());
            s.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public ByteBuffer remove(Object _key) {
        ByteBuffer key = (ByteBuffer)_key;
        try {
            Socket s = conf.getShard(key.array()).openSocket();
            DataOutputStream out = new DataOutputStream(s.getOutputStream());
            out.write("del".getBytes());
            out.writeInt(key.limit());
            out.write(key.array());
            s.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
        for (Entry<String, ShardingConfiguration.ShardItem> shard : conf.shards.entrySet()) {
            Socket s = null;
            try {
                s = shard.getValue().openSocket();
                s.getOutputStream().write("clr".getBytes());
                s.close();
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
}

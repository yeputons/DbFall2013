package net.yeputons.cscenter.dbfall2013.scaling;

import net.yeputons.cscenter.dbfall2013.engines.hashtrie.HashTrieEngine;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:10
 * To change this template use File | Settings | File Templates.
 */
public class ShardingNode {
    private static byte[] readBytes(DataInputStream in) throws IOException {
        int len = in.readInt();
        if (len == -1) return null;
        byte[] res = new byte[len];
        in.readFully(res);
        return res;
    }
    private static void writeBytes(DataOutputStream out, byte[] data) throws IOException {
        if (out == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(data.length);
            out.write(data);
        }
    }

    HashTrieEngine engine;
    protected void processClient(Socket clientSocket) throws Exception {
        DataInputStream in = new DataInputStream(clientSocket.getInputStream());
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

        while (true) {
            byte[] cmd = new byte[3];
            in.readFully(cmd);
            if (Arrays.equals(cmd, "clr".getBytes())) {
                engine.clear();
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "siz".getBytes())) {
                out.write("ok".getBytes());
                out.writeInt(engine.size());
            } else if (Arrays.equals(cmd, "del".getBytes())) {
                int len = in.readInt();
                byte[] key = readBytes(in);
                engine.remove(key);
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "put".getBytes())) {
                byte[] value = readBytes(in);
                byte[] key = readBytes(in);
                engine.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "get".getBytes())) {
                byte[] key = readBytes(in);
                ByteBuffer res = engine.get(ByteBuffer.wrap(key));
                out.write("ok".getBytes());
                writeBytes(out, res == null ? null : res.array());
            } else {
                out.write("no".getBytes());
                writeBytes(out, "Invalid command".getBytes());
            }
        }
    }

    public void run(File storage, InetAddress bindTo, int port) throws Exception {
        engine = new HashTrieEngine(storage);
        ServerSocket serverSocket = new ServerSocket(port, 0, bindTo);

        while (true) {
            final Socket clientSocket = serverSocket.accept();
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        processClient(clientSocket);
                    } catch (EOFException e) {
                    } catch (SocketException e) {
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    try {
                        clientSocket.close();
                    } catch (SocketException e) {
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }).run();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Arguments: <storage file> <ip to listen on> <port>\n");
            System.exit(1);
        }
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        new ShardingNode().run(new File(args[0]), InetAddress.getByName(host), port);
    }
}

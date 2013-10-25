package net.yeputons.cscenter.dbfall2013.scaling;

import net.yeputons.cscenter.dbfall2013.engines.hashtrie.HashTrieEngine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import net.yeputons.cscenter.dbfall2013.util.DataInputStream;
import net.yeputons.cscenter.dbfall2013.util.DataOutputStream;

import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:10
 * To change this template use File | Settings | File Templates.
 */
public class ShardingNode {
    static final Logger log = LoggerFactory.getLogger(ShardingNode.class);

    File storage;
    HashTrieEngine engine;

    protected void processClient(Socket clientSocket) throws Exception {
        DataInputStream in = new DataInputStream(clientSocket.getInputStream());
        DataOutputStream out = new DataOutputStream(clientSocket.getOutputStream());

        while (true) {
            byte[] cmd = new byte[3];
            in.readFully(cmd);
            if (Arrays.equals(cmd, "clr".getBytes())) {
                synchronized (engine) {
                    engine.clear();
                }
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "siz".getBytes())) {
                out.write("ok".getBytes());
                int siz;
                synchronized (engine) {
                    siz = engine.size();
                }
                out.writeInt(siz);
            } else if (Arrays.equals(cmd, "del".getBytes())) {
                byte[] key = in.readArray();
                synchronized (engine) {
                    engine.remove(ByteBuffer.wrap(key));
                }
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "put".getBytes())) {
                byte[] value = in.readArray();
                byte[] key = in.readArray();
                synchronized (engine) {
                    engine.put(ByteBuffer.wrap(key), ByteBuffer.wrap(value));
                }
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "get".getBytes())) {
                byte[] key = in.readArray();
                ByteBuffer res;
                synchronized (engine) {
                    res = engine.get(ByteBuffer.wrap(key));
                }
                out.write("ok".getBytes());
                out.writeArray(res == null ? null : res.array());
            } else if (Arrays.equals(cmd, "hi!".getBytes())) {
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "key".getBytes())) {
                out.write("ok".getBytes());
                synchronized (engine) {
                    out.writeInt(engine.size());
                    for (ByteBuffer key : engine.keySet())
                        out.writeArray(key.array());
                }
            } else if (Arrays.equals(cmd, "its".getBytes())) {
                out.write("ok".getBytes());
                synchronized (engine) {
                    out.writeInt(engine.size());
                    for (Map.Entry<ByteBuffer, ByteBuffer> entry : engine.entrySet()) {
                        out.writeArray(entry.getKey().array());
                        out.writeArray(entry.getValue().array());
                    }
                }
            } else if (Arrays.equals(cmd, "pak".getBytes())) {
                synchronized (engine) {
                    engine.runCompaction();
                }
                out.write("ok".getBytes());
            } else if (Arrays.equals(cmd, "dwn".getBytes())) {
                out.write("ok".getBytes());
                this.stop();
            } else {
                out.write("no".getBytes());
                out.writeArray("Invalid command".getBytes());
            }
            out.flush();
        }
    }

    Set<Socket> clients;
    protected ServerSocket serverSocket;
    protected volatile boolean isRunning;

    public void run(File storage_, InetSocketAddress bindTo) throws Exception {
        storage = storage_;

        log.info("Starting node on {}, please be patient...", bindTo);
        isRunning = true;
        engine = new HashTrieEngine(storage);
        serverSocket = new ServerSocket(bindTo.getPort(), 0, bindTo.getAddress());

        clients = new HashSet<Socket>();

        log.info("Node is up and ready to accept connections");
        while (true) {
            try {
                final Socket clientSocket = serverSocket.accept();
                log.debug("New client from {}:{} connected", clientSocket.getInetAddress(), clientSocket.getPort());
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        synchronized (clients) {
                            if (!isRunning) {
                                try {
                                    clientSocket.close();
                                } catch (IOException e) {
                                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                                }
                                return;
                            }
                            clients.add(clientSocket);
                        }

                        try {
                            processClient(clientSocket);
                        } catch (EOFException e) {
                        } catch (SocketException e) {
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        log.debug("Client from {}:{} disconnected", clientSocket.getInetAddress(), clientSocket.getPort());

                        try {
                            clientSocket.close();
                        } catch (SocketException e) {
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        synchronized (clients) {
                            clients.remove(clientSocket);
                        }
                    }
                }).start();
            } catch (SocketException e) {
            }
            if (!isRunning) break;
        }
        log.info("Terminating connections...");
        synchronized (clients) {
            for (Socket s : clients)
                s.close();
        }
        serverSocket.close();
        log.info("Terminating engine...");
        synchronized (engine) {
            engine.close();
        }
        isRunning = false;
        log.info("Node is down");
    }
    public void stop() {
        isRunning = false;
        try {
            serverSocket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Arguments: <storage file> <ip to listen on> <port>\n");
            System.exit(1);
        }
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        new ShardingNode().run(new File(args[0]), new InetSocketAddress(host, port));
    }
}

package net.yeputons.cscenter.dbfall2013.clients;

import net.yeputons.cscenter.dbfall2013.scaling.RouterCommunicationException;
import net.yeputons.cscenter.dbfall2013.scaling.ShardDescription;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingConfiguration;
import net.yeputons.cscenter.dbfall2013.util.DataInputStream;
import net.yeputons.cscenter.dbfall2013.util.DataOutputStream;

import java.io.File;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 25.10.13
 * Time: 0:54
 * To change this template use File | Settings | File Templates.
 */
public class ClusterManager {
    public static void main(String[] args) throws Exception {
        System.out.println("Welcome to " + ClusterManager.class.getName() + "!");
        System.out.println("Type 'help' for help");

        ShardingConfiguration conf;
        File f = new File("sharding.yaml");
        conf = new ShardingConfiguration();
        conf.readFromFile(f);

        Scanner in = new Scanner(System.in);
        while (true) {
            System.out.print(">>> ");

            String line;
            try {
                line = in.nextLine();
            } catch (NoSuchElementException e) {
                break;
            }
            if (line.isEmpty()) continue;

            if (line.equals("quit")) {
                break;
            } else if (line.equals("help")) {
                showHelp();
                continue;
            } else if (line.equals("list_nodes")) {
                System.out.printf("Current nodes:\n");
                for (Map.Entry<String, ShardDescription> entry : conf.shards.entrySet()) {
                    ShardDescription shard = entry.getValue();
                    boolean online = false;
                    try {
                        Socket s = shard.openSocket();
                        online = true;
                        s.close();
                    } catch (ConnectException e) {
                    }
                    System.out.printf("%s..%s: %s at %s\n", shard.startHash, shard.endHash, online ? "online" : "offline", shard.address);
                }
            } else {
                String[] tokens = line.split(" ");
                if (tokens.length != 2) {
                    System.out.printf("ERROR: invalid command\n");
                    continue;
                }
                ShardDescription shard = conf.shards.floorEntry(tokens[0]).getValue();
                if (shard == null || tokens[0].compareTo(shard.endHash) > 0) {
                    System.out.printf("ERROR: unable to find shard responsible for '" + tokens[0] + "'\n");
                    continue;
                }

                Socket s = null;
                try {
                    s = shard.openSocket();
                    DataInputStream sin = new DataInputStream(s.getInputStream());
                    DataOutputStream sout = new DataOutputStream(s.getOutputStream());
                    if (tokens[1].equals("say_hi")) {
                        sout.write("hi!".getBytes());
                    }
                    byte[] res = new byte[2];
                    sin.readFully(res);
                    if (Arrays.equals(res, "ok".getBytes())) {
                        System.out.printf("SUCCESS\n");
                    } else if (Arrays.equals(res, "no".getBytes())) {
                        byte[] answer = sin.readArray();
                        System.out.printf("ERROR: server return an error: %s\n", new String(answer));
                    } else {
                        System.out.printf("ERROR: server returned unknown response\n");
                    }
                } catch (ConnectException e) {
                    System.out.printf("ERROR: unable to connect\n");
                } catch (IOException e) {
                    System.out.printf("ERROR: unable to read/write data\n");
                }
                if (s != null) s.close();
            }
        }
        in.close();
    }

    protected static void showHelp() {
        System.out.printf("Not implemented yet\n");
    }
}

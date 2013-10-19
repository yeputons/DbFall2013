package net.yeputons.cscenter.dbfall2013.clients;

import net.yeputons.cscenter.dbfall2013.scaling.ShardDescription;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingConfiguration;
import net.yeputons.cscenter.dbfall2013.scaling.ShardingNode;

import java.io.File;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: e.suvorov
 * Date: 13.10.13
 * Time: 22:33
 * To change this template use File | Settings | File Templates.
 */
public class Bootstrapper {
    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            System.err.println(
                    "Please, specify what to run in format: <run item> [<its arguments>]\n" +
                    "Available items: client, node, all_nodes"
            );
            System.exit(1);
        }

        if (args[0].equals("client")) {
            ConsoleClient.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (args[0].equals("node")) {
            ShardingNode.main(Arrays.copyOfRange(args, 1, args.length));
        } else if (args[0].equals("all_nodes")) {
            if (args.length > 1) {
                System.err.println(
                        "'all_nodes' does not take any arguments\n" +
                        "It reads sharding configuration from ./sharding.yaml and" +
                        "puts storages to ./storage-shard-<startHash>.trie"
                );
                System.exit(1);
            }
            File f = new File("sharding.yaml");
            ShardingConfiguration conf = new ShardingConfiguration();
            conf.readFromFile(f);

            for (Map.Entry<String, ShardDescription> entry : conf.shards.entrySet()) {
                final File storage = new File("storage-shard-" + entry.getKey() + ".trie");
                final ShardDescription descr = entry.getValue();
                final ShardingNode node = new ShardingNode();
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            node.run(storage, descr.address);
                        } catch (Exception e) {
                            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                        }
                    }
                }).start();
            }
        }
    }
}

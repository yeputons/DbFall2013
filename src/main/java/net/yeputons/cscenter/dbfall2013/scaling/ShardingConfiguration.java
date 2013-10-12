package net.yeputons.cscenter.dbfall2013.scaling;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.URL;
import java.security.InvalidParameterException;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:11
 * To change this template use File | Settings | File Templates.
 */
public class ShardingConfiguration {
    public class ShardItem {
        public String host;
        public int port;
    }

    public TreeMap<String, ShardItem> shards;

    public void readFromFile(File f) throws FileNotFoundException {
        shards = new TreeMap<String, ShardItem>();
        Yaml yaml = new Yaml();
        Map data = (Map)yaml.load(new FileInputStream(f));
        for (Object _node : (List)data.get("sharding")) {
            Map node = (Map)_node;
            System.out.println(node);

            String startHash = (String) node.get("startHash");
            ShardItem item = new ShardItem();
            item.host = (String) node.get("host");
            item.port = (Integer) node.get("port");
            shards.put(startHash, item);
        }

        final String zeroHash = "00000" + "00000" + "00000" + "00000";
        if (shards.floorKey(zeroHash) == null) {
            throw new InvalidParameterException("Zero hash is not included in any shard");
        }
    }
}

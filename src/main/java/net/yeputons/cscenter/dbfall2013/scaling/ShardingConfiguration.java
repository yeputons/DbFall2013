package net.yeputons.cscenter.dbfall2013.scaling;

import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.InvalidParameterException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA.
 * User: Egor Suvorov
 * Date: 12.10.13
 * Time: 19:11
 * To change this template use File | Settings | File Templates.
 */
public class ShardingConfiguration {
    protected final static MessageDigest md;
    static {
        try {
            md = MessageDigest.getInstance("SHA-1");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    public final static int DEFAULT_PORT = 33131;

    public TreeMap<String, ShardDescription> shards;

    public ShardingConfiguration() {
        shards = new TreeMap<String, ShardDescription>();
    }

    public ShardDescription getShard(byte[] key) {
        StringBuilder digest = new StringBuilder();
        byte[] digestArr;
        synchronized (md) {
            digestArr = md.digest(key);
        }
        for (byte b : digestArr)
            digest.append(String.format("%02x", b & 0xFF));
        return shards.floorEntry(digest.toString()).getValue();
    }

    static final String hashChars = "0123456789abcdef";
    static final int hashLen = 20;
    protected String getFirstHashGe(String startHash) {
        String res = "";
        for (int i = 0; i < hashLen; i++) {
            boolean found = false;
            for (int i2 = 0; i2 < hashChars.length(); i2++) {
                String cand = res + hashChars.charAt(i2);
                for (int i3 = i + 1; i3 < hashLen; i3++)
                    cand += hashChars.charAt(0);
                if (startHash.compareTo(cand) <= 0) {
                    found = true;
                    res += hashChars.charAt(i2);
                    break;
                }
            }
            if (!found) throw new IllegalArgumentException("Unable to found hash >= than '" + startHash + "'");
        }
        return res;
    }

    public void readFromFile(File f) throws FileNotFoundException, URISyntaxException {
        shards = new TreeMap<String, ShardDescription>();
        Yaml yaml = new Yaml();
        Map data = (Map)yaml.load(new FileInputStream(f));

        TreeSet<String> startHashes = new TreeSet<String>();
        for (Object _node : (List)data.get("sharding")) {
            Map node = (Map)_node;

            String startHash = (String) node.get("startHash");
            startHash = startHash.toLowerCase();

            ShardDescription item = new ShardDescription();
            URI uri = new URI("my://" + (String) node.get("address"));
            item.address = new InetSocketAddress(
                    uri.getHost() == null ? "localhost" : uri.getHost(),
                    uri.getPort() < 0 ? DEFAULT_PORT : uri.getPort()
            );
            item.startHash = getFirstHashGe(startHash);
            if (startHashes.contains(item.startHash))
                throw new InvalidParameterException("Hash '" + startHash + "' appears as start one at least twice");
            startHashes.add(item.startHash);
            shards.put(startHash, item);
        }

        for (Map.Entry<String, ShardDescription> item : shards.entrySet()) {
            ShardDescription shard = item.getValue();
            String nextStart = startHashes.higher(shard.startHash);
            if (nextStart == null) {
                shard.endHash = "";
                for (int i = 0; i < hashLen; i++)
                    shard.endHash += hashChars.charAt(hashChars.length() - 1);
            } else {
                char[] result = new char[hashLen];
                nextStart.getChars(0, hashLen, result, 0);
                boolean success = false;
                for (int i = hashLen - 1; i >= 0; i--) {
                    int curId = hashChars.indexOf(result[i]);
                    if (curId == 0) {
                        result[i] = hashChars.charAt(hashChars.length() - 1);
                    } else {
                        result[i] = hashChars.charAt(curId - 1);
                        success = true;
                        break;
                    }
                }
                assert success;
                shard.endHash = new String(result);
            }
        }

        final String zeroHash = "00000" + "00000" + "00000" + "00000";
        if (shards.floorKey(zeroHash) == null) {
            throw new InvalidParameterException("Zero hash is not included in any shard");
        }
    }
}

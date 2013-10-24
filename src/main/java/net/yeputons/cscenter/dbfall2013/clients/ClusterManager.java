package net.yeputons.cscenter.dbfall2013.clients;

import net.yeputons.cscenter.dbfall2013.scaling.ShardingConfiguration;

import java.io.File;
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
            }
        }
        in.close();
    }

    protected static void showHelp() {
        System.out.printf("Not implemented yet\n");
    }
}

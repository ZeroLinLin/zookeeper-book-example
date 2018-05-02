package com.zero;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Date;

public class MyAdminClient implements Watcher {
    ZooKeeper zk;
    String hostPort;

    public MyAdminClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void start() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    void listState() throws KeeperException, InterruptedException {
        try {
            Stat stat = new Stat();
            byte[] masterData = zk.getData("/master", false, stat);
            Date startDate = new Date(stat.getCtime());//通过Stat结构可以获得当前主节点成为主节点的时间信息

            System.out.println("Master:" + new String(masterData) + "since " + startDate);
        } catch (KeeperException.NoNodeException e) {
            System.out.println("No master");
        }

        System.out.println("Workers:");
        for (String w : zk.getChildren("/workers", false)) {
            byte data[] = zk.getData("/workers/" + w, false, null);

            String state = new String(data); // 数据表示从节点的状态
            System.out.println("\t" + w + ":" + state);
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
        MyAdminClient myAdminClient = new MyAdminClient("127.0.0.1:2181");
        myAdminClient.start();
        myAdminClient.listState();
    }
}

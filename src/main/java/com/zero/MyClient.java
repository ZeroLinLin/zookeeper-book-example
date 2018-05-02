package com.zero;

import org.apache.zookeeper.*;

import java.io.IOException;

public class MyClient implements Watcher {
    ZooKeeper zk;
    String hostPort;

    public MyClient(String hostPort) {
        this.hostPort = hostPort;
    }

    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    String queueCommand(String command) throws Exception {
        while (true) {
            String name = "";
            try {
                name = zk.create("/tasks/task-",
                        command.getBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT_SEQUENTIAL);
                return name; // 返回带有序号的节点名称
            } catch (KeeperException.NodeExistsException e) {
                throw new Exception(name + "already apperars to be running");
            } catch (KeeperException.ConnectionLossException e) {
                //连接丢失需要重试，视情况而定，任务是至少执行一次还是最多执行一次
            }
        }
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        System.out.println(watchedEvent);
    }

    public static void main(String[] args) throws Exception {
        MyClient client = new MyClient("127.0.0.1:2181");
        client.startZK();
        String name = client.queueCommand("fix something");
        System.out.println(name);
    }
}

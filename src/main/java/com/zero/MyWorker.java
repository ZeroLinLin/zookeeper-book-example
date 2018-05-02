package com.zero;

import org.apache.zookeeper.*;
import org.apache.zookeeper.book.Master;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.Random;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class MyWorker implements Watcher,Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    private ZooKeeper zk;
    private String hostPort;
    private String serverId = Integer.toHexString((new Random()).nextInt());
    private volatile boolean connected = false;
    private volatile boolean expired = false;

    String name;
    String status;
    /*
      通常，阻塞ZooKeeper客户端回调线程不是一个好的办法。
      我们使用一个线程池来处理回调的执行。
     */
    private ThreadPoolExecutor executor;
    /**
     * 创建一个从节点实例
     * @param hostPort
     */
    public MyWorker(String hostPort) {
        this.hostPort = hostPort;
        this.executor = new ThreadPoolExecutor(1, 1, 1000L,
                TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(200));
    }

    /**
     * 处理会话事件，如：连接，失去连接
     * @param e
     */
    @Override
    public void process(WatchedEvent e) {
        LOG.info(e.toString() + ", " + hostPort);
        if (e.getType() == Event.EventType.None) {
            switch (e.getState()) {
                case SyncConnected:
                    /*
                      注册到ZooKeeper
                     */
                    connected = true;
                    break;
                case Disconnected:
                    connected = false;
                    break;
                case Expired:
                    expired = true;
                    connected = false;
                    LOG.error("会话过期");
                    break;
                default:
                    break;
            }
        }
    }

    /**
     * 关闭ZooKeeper会话
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }

    /**
     * 创建一个ZooKeeper会话
     * @throws IOException
     */
    void startZK() throws IOException {
        zk = new ZooKeeper(hostPort, 15000, this);
    }
    /**
     * 检查客户端是否连接上
     * @return
     */
    public boolean isConnected() {
        return connected;
    }
    /**
     * 检查ZooKeeper会话是否过期
     * @return
     */
    public boolean isExpired() {
        return expired;
    }

    /**
     * 这里只是创建一个/assign父节点，用于将任务分配给从节点
     */
    public  void bootstrap() {
        createAssignNode();
    }
    void createAssignNode() {
        zk.create("/assign/worker-" + serverId,
                new byte[0],
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createAssignCallback,
                null);
    }
    //TODO

    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    register();
                    break;
                case OK:
                    LOG.info("Registered successfully:" + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("Already registered:" + serverId);
                    break;
                default:
                    LOG.error("Something went wrong:" + KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }

    AsyncCallback.StatCallback statusUpdateCallback = new AsyncCallback.StatCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    updateStatus((String)ctx);
                    return;
            }
        }
    };
    synchronized private void updateStatus(String status) {
        //进行一个状态更新请求前，需要先获得当前状态，否则就要放弃更新
        //这样判断的目的：因连接丢失重新请求时，可能会导致整个时序中出现空隙？？
        if (status == this.status) {
            // -1 表示禁止版本号检查
            zk.setData("/workers"+name, status.getBytes(), -1, statusUpdateCallback, status);
        }
    }
    public void setStatus(String status) {
        this.status = status;//状态信息保存到本地变量中
        updateStatus(status);
    }

    public static void main(String[] args) throws Exception{
        MyWorker w = new MyWorker("127.0.0.1:2181");
        w.startZK();
        w.register();
        Thread.sleep(30000);
    }

}

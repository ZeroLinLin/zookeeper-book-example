package com.zero;

import org.apache.zookeeper.*;
import org.apache.zookeeper.book.Master;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
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
    // 从节点名称
    private String name;
    // 从节点状态
    private String status;
    // 执行次数
    private int executionCount;
    /*
      通常，阻塞ZooKeeper客户端回调线程不是一个好的办法。
      我们使用一个线程池来处理回调的执行。
     */
    private ThreadPoolExecutor executor;

    protected ChildrenCache assignedTasksCache = new ChildrenCache();

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
        LOG.info("ZooKeeper会话关闭");
        try {
            zk.close();
        } catch (InterruptedException e) {
            LOG.warn("关闭ZooKeeper会话时发生中断");
        }
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
     * 这里只是在/assign父节点下创建一个worker节点（/assign/worker-`serverId`）
     * 表示可以分配任务给的节点，也就是可分配节点中注册从节点
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
    AsyncCallback.StringCallback createAssignCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                    连接失败需要重试，保证节点存在,如果节点存在会返回NODEEXISTS事件
                     */
                    createAssignNode();
                    break;
                case OK:
                    LOG.info("成功在可分配节点中/assign注册从节点：worker-" + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("可分配节点中从节点/assign已经注册：worker-" + serverId);
                    break;
                default:
                    LOG.error("可分配节点中/assign注册从节点发生错误：",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 从节点列表/workkers中注册从节点
     */
    void register() {
        zk.create("/workers/worker-" + serverId,
                "Idle".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                createWorkerCallback,
                null);
    }
    AsyncCallback.StringCallback createWorkerCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                    连接失败需要重试，保证节点存在,如果节点存在会返回NODEEXISTS事件
                     */
                    register();
                    break;
                case OK:
                    LOG.info("成功在从节点列表/workkers中注册从节点：worker-" + serverId);
                    break;
                case NODEEXISTS:
                    LOG.warn("从节点列表/workkers中已经存在注册从节点：worker-" + serverId);
                    break;
                default:
                    LOG.error("从节点列表/workkers中注册从节点发生错误",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 阻塞方法，更新zookeeper中状态
     * @param status
     */
    synchronized private void updateStatus(String status) {
        //进行一个状态更新请求前，需要先获得当前状态，否则就要放弃更新
        //这样判断的目的：因连接丢失重新请求时，可能会导致整个时序中出现空隙？？
        if (status == this.status) {
            // -1 表示禁止版本号检查
            zk.setData("/workers" + name, status.getBytes(), -1, statusUpdateCallback, status);
        }
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
    /**
     * 设置从节点状态
     * 会更新对于zookeeper中/workers列表中从节点的状态
     * @param status
     */
    public void setStatus(String status) {
        this.status = status;//状态信息保存到本地变量中
        updateStatus(status);
    }

    synchronized void changeExecutionCount(int countChange) {
        executionCount += countChange;
        if (executionCount == 0 && countChange < 0) {
            // 我们变成了idle
            setStatus("Idle");
        }
        if (executionCount ==1 && countChange > 0) {
            // 我们变成了working
            setStatus("Working");
        }
    }

    /*
     ***************************************
     ***************************************
     * 等待新任务进行分配的方法*
     ***************************************
     ***************************************
     */
    void getTasks() {
        zk.getChildren("/assign/worker-" + serverId,
                getTasksWatcher,
                getTasksCallback,
                null);
    }
    Watcher getTasksWatcher = new Watcher() {
        @Override
        public void process(WatchedEvent e) {
            if (e.getType() == Event.EventType.NodeChildrenChanged) {
                assert new String("/assign/worker-" + serverId).equals(e.getPath());
                getTasks();
            }
        }
    };
    AsyncCallback.ChildrenCallback getTasksCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getTasks();
                    break;
                case OK:
                    if (children != null) {
                        executor.execute(new Runnable() {
                            List<String> children;
                            DataCallback cb;
                            /*
                             初始化匿名类输入
                             */
                            public Runnable init(List<String> children, DataCallback cb) {
                                this.children = children;
                                this.cb = cb;
                                return this;
                            }
                            @Override
                            public void run() {
                                if (children == null) {
                                    return;
                                }
                                LOG.info("循环进入任务");
                                setStatus("Working");
                                for (String task : children) {
                                    LOG.trace("新任务：{}",task);
                                    zk.getData("/assign/worker-" + serverId + "/" + task,
                                            false,
                                            cb,
                                            task);
                                }
                            }
                        }.init(assignedTasksCache.addedAndSet(children), taskDataCallback));
                    }
                    break;
                default:
                    LOG.error("获取任务列表发生错误",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };
    AsyncCallback.DataCallback taskDataCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    zk.getData(path, false, taskDataCallback, null);
                    break;
                case OK:
                    /*
                      zhi执行任务在这里是简单的输出控制台信息
                     */
                    executor.execute(new Runnable() {
                        byte[] data;
                        Object ctx;
                        /*
                          初始匿名类变量
                         */
                        public Runnable init(byte[] data, Object ctx) {
                            this.data = data;
                            this.ctx = ctx;
                            return this;
                        }
                        @Override
                        public void run() {
                            // 执行任务
                            LOG.info("执行任务：" + new String(data));
                            // 任务完成，修改状态，从节点中分配的任务列表中删除该任务
                            taskStatusCreate("/status/" + (String)ctx);
                            assignWorkerTaskDelete("/assign/worker-" + serverId + "/" + (String)ctx);
                        }
                    }.init(data, ctx));

                    break;
                default:
                    LOG.error("获取任务数据发生错误：",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 创建任务完成状态
     * @param path
     */
    void taskStatusCreate(String path) {
        zk.create(path,
                "done".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                taskStatusCreateCallback,
                null);
    }
    AsyncCallback.StringCallback taskStatusCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    taskStatusCreate(path);
                    break;
                case OK:
                    LOG.info("成功创建任务完成状态：" + name);
                    break;
                case NODEEXISTS:
                    LOG.warn("任务完成状态已存在：" + path);
                    break;
                default:
                    LOG.error("创建任务完成状态发生错误：",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 从节点完成的任务，需要在分配给从节点的任务列表中删除
     * @param path
     */
    void assignWorkerTaskDelete(String path) {
        zk.delete(path,
                -1,
                assignWorkerTaskDeleteCallback,
                null);
    }
    AsyncCallback.VoidCallback assignWorkerTaskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    assignWorkerTaskDelete(path);
                    break;
                case OK:
                case NONODE:
                    LOG.info("分配给从节点的任务列表中已删除完成任务：" + path);
                    break;
                default:
                    LOG.error("分配给从节点的任务列表中删除完成任务时发生错误",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    public static void main(String[] args) throws Exception{
        MyWorker w = new MyWorker("127.0.0.1:2181");
        w.startZK();

        while (!w.isConnected()) {
            Thread.sleep(100);
        }
        /*
          创建必要的节点
         */
        w.bootstrap();

        /*
          注册从节点，以便主节点知道它存在
         */
        w.register();

        /*
          获取分配给从节点的任务
         */
        w.getTasks();

        while ((!w.isExpired())) {
            Thread.sleep(1000);
        }
    }

}

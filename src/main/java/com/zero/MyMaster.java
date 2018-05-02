package com.zero;

import org.apache.zookeeper.*;
import org.apache.zookeeper.book.Master;
import org.apache.zookeeper.book.recovery.RecoveredAssignments;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Random;

/**
 * This class implements the master of the master-worker example we use
 * throughout the book. The master is responsible for tracking the list of
 * available workers, determining when there are new tasks and assigning
 * them to available workers.
 * 这个类实现了我们在整本书中使用的“主-从节点”示例的主节点（master为主节点管理作用，worker为从节点是任务执行者）。
 * master负责跟踪可用员工的列表，确定何时有新任务，并将其分配给可用的员工。
 *
 * The flow without crashes is like this. The master reads the list of
 * available workers and watch for changes to the list of workers. It also
 * reads the list of tasks and watches for changes to the list of tasks.
 * For each new task, it assigns the task to a worker chosen at random.
 * 没有崩溃的流程是这样的。master读取可用从节点的列表，并监视workers的列表.
 * 它也读取任务列表，并且监视任务列表。
 * 对于每个新任务，它将任务分配给一个随机的worker。
 *
 * Before exercising the role of master, this ZooKeeper client first needs
 * to elect a primary master. It does it by creating a /master znode. If
 * it succeeds, then it exercises the role of master. Otherwise, it watches
 * the /master znode, and if it goes away, it tries to elect a new primary
 * master.
 * 在master发挥作用之前，这个zookeeper客户端首先需要需要进行master选举。它通过创建一个/master节点。
 * 如果成功，然后它就扮演master角色。否则，它监视这个/master节点，如果节点消失，他会尝试选举一个新的主master。
 *
 * The states of this client are three: RUNNING, ELECTED, NOTELECTED.
 * RUNNING means that according to its view of the ZooKeeper state, there
 * is no primary master (no master has been able to acquire the /master lock).
 * If some master succeeds in creating the /master znode and this master learns
 * it, then it transitions to ELECTED if it is the primary and NOTELECTED
 * otherwise.
 * 客户端的状态有3种：RUNNING, ELECTED, NOTELECTED。
 * RUNNING表示没有主节点(即/master节点不存在，其中/master作为一个锁)。
 * 如果有其他master成功创建/master节点，而这个master获知这个情况，那么这个master状态为NOTELECTED。
 * 那么如果这个maseter是主节点（创建/maseter节点成功），并且状态是NOTELECTED，那么它的状态就会转变成ELECTED，否则就会被忽略。
 *
 * Because workers may crash, this master also needs to be able to reassign
 * tasks. When it watches for changes in the list of workers, it also
 * receives a notification when a znode representing a worker is gone, so
 * it is able to reassign its tasks.
 * 因为workers可能会崩溃，所以这个master也需要能够重新分配任务。
 * 当master监视workers列表中的变化时，它也会受到一个worder节点按消失时发出的通知，
 * 这样master就能重新分配任务。
 *
 * A primary may crash too. In the case a primary crashes, the next primary
 * that takes over the role needs to make sure that it assigns and reassigns
 * tasks that the previous primary hasn't had time to process.
 * 一个主节点也可能崩溃。在主节点崩溃时，下一个主节点接管该角色时，需要确保分配和重新分配那些上一个主节点还没来得及处理的任务。
 */
public class MyMaster implements Watcher,Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(Master.class);

    /*
     * 一个主节点能是运行中的主节点，已选举主节点，没有选举主节点
     */
    enum MasterStates {RUNNING, ELECTED, NOTELECTED};

    private Random random = new Random(this.hashCode());
    ZooKeeper zk;
    String hostPort;
    private String serverId = Integer.toHexString(random.nextInt());
    private volatile boolean connected = false;
    private volatile  boolean expired = false;
    private volatile MasterStates state = MasterStates.RUNNING;

    protected ChildrenCache tasksCache;
    protected ChildrenCache workersCache;

    /**
     * 主节点构造方法
     * @param hostPort
     */
    public MyMaster(String hostPort) {
        this.hostPort = hostPort;
    }

    /**
     * 创建一个zookeeper会话
     * @throws IOException
     */
    void startZK() throws IOException{
        zk = new ZooKeeper(hostPort, 15000, this);
    }

    /**
     * 关闭一个zookeeper会话
     * @throws InterruptedException
     */
    void stopZK() throws InterruptedException {
        zk.close();
    }

    /**
     * Master实现了Watcher接口，实现了该接口的process方法。
     * 我们使用它来处理会话的不同状态事件。
     * @param e
     */
    @Override
    public void process(WatchedEvent e) {
        LOG.info("master process 事件: " + e.toString());
        if (e.getType() == Event.EventType.None) {
           switch (e.getState()) {
               case SyncConnected:
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
        if (zk != null) {
            try {
                zk.close();
            } catch (InterruptedException e) {
                LOG.warn("关闭zookeeper会话发生中断");
            }
        }
    }

    /**
     * 这个方法创建一些例子中需要的父节点。主节点重启的情况，这个方法不需要再次执行。
     */
    public void bootstrap() {
        createParent("/workers", new byte[0]);
        createParent("/assign", new byte[0]);
        createParent("/tasks", new byte[0]);
        createParent("/status", new byte[0]);

    }
    public void createParent(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                createParentCallback,
                data);

    }
    AsyncCallback.StringCallback createParentCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    /*
                     * 连接丢失时需要重试，重试不会有问题，因为如果节点存在的话会返回NODEEXISTS事件
                     */
                    createParent(path, (byte[]) ctx);
                    break;
                case OK:
                    LOG.info("成功创建父节点:" + path);
                    break;
                case NODEEXISTS:
                    LOG.warn("父节点已经存在：" + path);
                    break;
                default:
                    LOG.error("创建父节点时发生错误：" + KeeperException.create(KeeperException.Code.get(rc)), path);
            }
        }
    };

    /**
     * 检查客户端是否已连接（zookeeper）
     * @return
     */
    boolean isConnected() {
        return connected;
    }

    /**
     * 检查客户端连接会话是否过期
     * @return
     */
    boolean isExpired() {
        return expired;
    }

    /**
     * 获取master状态
     * @return
     */
    MasterStates getState() {
        return state;
    }

    /*
     **************************************
     **************************************
     * 1、主节点选举相关的方法*
     **************************************
     **************************************
     */

    /*
     * 下面是回调实现的过程。
     * 我们尝试创建一个/master节点作为锁节点。如果成功，它就会变成一个leader角色。
     * 然而，这里有很多异常情况需要我们考虑并处理。
     *
     * 首先，在得到服务端响应时可能连接丢失，我们要考虑这个事件的发生。
     * 为了检查，我们尝试读取/master节点。如果它存在，我们需要检查这个master是否已经成为了主节点。
     * 如果没有，我们再次运行master。
     *
     * 第二种情况是我们发现这个/master节点已经存在。这种情况，我们调用exists方法去设置一个监视点，监视这个节点。
     */
    AsyncCallback.StringCallback masterCreateCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)){
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case OK:
                    state = MasterStates.ELECTED;
                    takeLeadeship();
                    break;
                case NODEEXISTS:
                    state = MasterStates.NOTELECTED;
                    masterExists();
                    break;
                default:
                    state = MasterStates.NOTELECTED;
                    break;
            }
            LOG.info("我" + (state == MasterStates.ELECTED ? "是" : "不是") + "the leader " + serverId);
        }
    };

    /**
     * /master节点是否存在，并监视
     */
    void masterExists() {
        zk.exists("/master",
                masterExistsWatcher,
                masterExistsCallback,
                null);
    }
    Watcher masterExistsWatcher = (e) -> {
      if (e.getType() == Event.EventType.NodeDeleted) {
          assert "/master".equals(e.getPath());
          runForMaster();
      }
    };
    AsyncCallback.StatCallback masterExistsCallback = (rc, path, ctx, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                masterExists();
                break;
            case OK:
                break;
            case NONODE:
                state = MasterStates.RUNNING;
                runForMaster();
                LOG.info("之前的主节点可能已经不存在，所以我们尝试作为再次主节点");
                break;
            default:
                checkMaster();
                break;
        }
    };

    /**
     * 承担主节点的角色
     * 获取从节点列表，恢复任务分配工作
     */
    void takeLeadeship() {
        LOG.info("承担主节点的角色，获取从节点列表");
        getWorkers();
        new RecoveredAssignments(zk).recover(new RecoveredAssignments.RecoveryCallback() {
            @Override
            public void recoveryComplete(int rc, List<String> tasks) {
                if (rc == RecoveredAssignments.RecoveryCallback.FAILED) {
                    LOG.error("恢复任务分配失败");
                } else {
                    LOG.info("已恢复任务分配");
                    getTasks();
                }
            }
        });
    }

    /**
     * 尝试创建/master节点，作为一个锁从而扮演leader角色
     */
    void runForMaster() {
        zk.create("/master",
                serverId.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL,
                masterCreateCallback,
                null
        );
    }

    /**
     * 检查/master是否存在
     * 不存在，尝试创建/master节点，自己作为主节点
     * 存在的话判断自己是被选中为主节点，还是选中了其他人为主节点
     */
    void checkMaster() {
        zk.getData("/master", false, masterCheckCallback, null);
    }
    AsyncCallback.DataCallback masterCheckCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch(KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    checkMaster();
                    break;
                case NONODE:
                    runForMaster();
                    break;
                case OK:
                    if (serverId.equals(new String(data))) {
                        state = MasterStates.ELECTED;
                        takeLeadeship();
                    } else {
                        state = MasterStates.NOTELECTED;
                        masterExists();
                    }
                    break;
                default:
                    LOG.error("检查主节点存在时发生错误：",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    /*
     ****************************************************
     ****************************************************
     * 2、处理从节点变化的方法*
     ****************************************************
     ****************************************************
     */

    /**
     * 获取从节点数量
     * @return
     */
    public int getWorkersSize() {
        if (workersCache == null) {
            return 0;
        } else {
            return workersCache.getList().size();
        }
    }
    /**
     * 获取从节点列表
     */
    void getWorkers() {
        zk.getChildren("/workers",
                workersChangeWatcher,
                workersGetChildrenCallback,
                null);
    }
    Watcher workersChangeWatcher = (e) -> {
      if (e.getType() == Event.EventType.NodeChildrenChanged) {
          assert "/workers".equals(e.getPath());
          getWorkers();
      }
    };
    AsyncCallback.ChildrenCallback workersGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getWorkers();
                break;
            case OK:
                LOG.info("成功获取从节点列表：" + children.size() + "个从节点");
                break;
            default:
                LOG.error("获取子节点失败",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };


    /*
     *******************
     *******************
     * 3、分配任务*
     *******************
     *******************
     */

    /**
     * 分配设置任务
     * @param children
     */
    void reassignAndSet(List<String> children) {
        List<String> toProcess;

        if (workersCache == null) {
            workersCache = new ChildrenCache(children);
            toProcess = null;
        } else {
            LOG.info("删除并设置");
            toProcess = workersCache.removedAndSet(children);
        }

        if (toProcess != null) {
            for (String worker : toProcess) {
                getAbsentWorkerTasks(worker);
            }
        }
    }

    /**
     * 获取故障从节点节点
     * @param worker
     */
    void getAbsentWorkerTasks(String worker) {
        zk.getChildren("/assign/" + worker,
                false,
                workerAssignmentCallback,
                null);
    }
    AsyncCallback.ChildrenCallback workerAssignmentCallback = new AsyncCallback.ChildrenCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, List<String> children) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getAbsentWorkerTasks(path);
                    break;
                case OK:
                    LOG.info("成功获取从节点的任务列表，" +
                    path + " 下有" +
                    children.size() + "个任务");
                    /*
                     * 重新分配已经分配给故障从节点的任务
                     */
                    for (String task : children) {
                        getDataReassign(path + "/" + task, task);
                    }
                    break;
                default:
                    LOG.error("获取子节点失败",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    /*
     ************************************************
     * 3、恢复分配给故障从节点的任务 *
     ************************************************
     */

    /**
     * 获取需要重新分配的任务并分配
     * @param path
     * @param task
     */
    void getDataReassign(String path, String task) {
        zk.getData(path,
                false,
                getDataReassignCallback,
                task);
    }
    AsyncCallback.DataCallback getDataReassignCallback = new AsyncCallback.DataCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    getDataReassign(path,(String)ctx);
                    break;
                case OK:
                    recreateTask(new RecreateTaskCtx(path, (String)ctx, data));
                    break;
                default:
                    LOG.error("获取数据时发生错误",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 重新创建任务上下文
     */
    class RecreateTaskCtx {
        String path;
        String task;
        byte[] data;
        RecreateTaskCtx(String path, String task, byte[] data) {
            this.path = path;
            this.task = task;
            this.data = data;
        }
    }

    /**
     * 重新创建任务
     * @param ctx
     */
    void recreateTask(RecreateTaskCtx ctx) {
        zk.create("/tasks/" + ctx.task,
                ctx.data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                recreateTaskCallback,
                ctx);
    }
    AsyncCallback.StringCallback recreateTaskCallback = new AsyncCallback.StringCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx, String name) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    recreateTask((RecreateTaskCtx)ctx);
                    break;
                case OK:
                    deleteAssignment(((RecreateTaskCtx)ctx).path);
                    break;
                case NODEEXISTS:
                    LOG.info("节点已经存在，如果没有被删除，之后会触发事件，所以保持尝试：" + path);
                    break;
                default:
                    LOG.error("重新创建任务失败",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };

    /**
     * 删除故障从节点分配到的任务
     * @param path
     */
    void deleteAssignment(String path) {
        zk.delete(path, -1, assignmentTaskDeleteCallback, null);
    }
    AsyncCallback.VoidCallback assignmentTaskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteAssignment(path);
                    break;
                case OK:
                    LOG.info("成功删除任务：" + path);
                    break;
                default:
                    LOG.error("尝试删除任务时发生错误",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };


    /*
     ******************************************************
     ******************************************************
     * 4、接收和分配新任务的方法*
     ******************************************************
     ******************************************************
     */

    /**
     * 获取任务列表
     */
    void getTasks() {
        zk.getChildren("/tasks",
                tasksChangeWatcher,
                taskGetChildrenCallback,
                null);
    }
    Watcher tasksChangeWatcher = (e) -> {
        if (e.getType() == Event.EventType.NodeChildrenChanged) {
            assert "/tasks".equals(e.getPath());
            getTasks();
        }
    };
    AsyncCallback.ChildrenCallback taskGetChildrenCallback = (rc, path, ctx, children) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getTasks();
                break;
            case OK:
                List<String> toProcess;
                if (tasksCache == null) {
                    tasksCache = new ChildrenCache(children);
                    toProcess = children;
                } else {
                    toProcess = tasksCache.addedAndSet(children);
                }

                if (toProcess != null) {
                    assignTasks(toProcess);
                }
                break;
            default:
                LOG.error("获取子节点失败：",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };
    /**
     * 任务分配
     * @param tasks
     */
    void assignTasks(List<String> tasks) {
        for (String task : tasks) {
            getTaskData(task);
        }
    }
    /**
     * 获取指定任务的数据
     * @param task
     */
    void getTaskData(String task) {
        zk.getData("/tasks/" + task,
                false,
                taskDataCallback,
                task);
    }
    AsyncCallback.DataCallback taskDataCallback = (rc, path, ctx, data, stat) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                getTaskData((String) ctx);
                break;
            case OK:
                /*
                 * 随机选择从节点
                 */
                List<String> list = workersCache.getList();
                String designnateWorker = list.get(random.nextInt(list.size()));
                /*
                 * 分配任务给随机选择的从节点
                 */
                String assignmentPath = "/assign/" + designnateWorker +
                        "/" + (String)ctx;
                LOG.info("分配路径" + assignmentPath);
                createAssignment(assignmentPath, data);
                break;
            default:
                LOG.error("尝试获取任务数据时发生错误：",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    /**
     * 创建任务分配
     * /assign/worker-id/task-num
     * @param path
     * @param data
     */
    void createAssignment(String path, byte[] data) {
        zk.create(path,
                data,
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                assignTaskCallback,
                data);
    }
    AsyncCallback.StringCallback assignTaskCallback = (rc, path, ctx, name) -> {
        switch (KeeperException.Code.get(rc)) {
            case CONNECTIONLOSS:
                // 创建分配节点，路径形式：/assign/worker-id/task-num
                createAssignment(path, (byte[])ctx);
                break;
            case OK:
                LOG.info("任务正常分配：" + name);
                // 成功分配的任务，删除/tasks下对应的
                deleteTask(name.substring(name.lastIndexOf("/") + 1));
                break;
            case NODEEXISTS:
                LOG.error("任务已经分配");
                break;
            default:
                LOG.error("尝试分配任务时发生错误：",
                        KeeperException.create(KeeperException.Code.get(rc), path));
        }
    };

    /**
     * 删除任务
     * @param name
     */
    void deleteTask(String name) {
        zk.delete("/task/" + name,-1, taskDeleteCallback, null);
    }
    AsyncCallback.VoidCallback taskDeleteCallback = new AsyncCallback.VoidCallback() {
        @Override
        public void processResult(int rc, String path, Object ctx) {
            switch (KeeperException.Code.get(rc)) {
                case CONNECTIONLOSS:
                    deleteTask(path);
                    break;
                case OK:
                    LOG.info("成功删除任务：" + path);
                    break;
                default:
                    LOG.error("尝试删除任务时发生错误",
                            KeeperException.create(KeeperException.Code.get(rc), path));
            }
        }
    };




    public static void main(String[] args) throws Exception{
        MyMaster m = new MyMaster("127.0.0.1:2181");
        m.startZK();

        while (!m.isConnected()) {
            Thread.sleep(100);
        }
        /*
        bootstrap()创建一些必要的节点
         */
        m.bootstrap();
        /*
        这里运行master
         */
        m.runForMaster();

        while (!m.isExpired()) {
            Thread.sleep(1000);
        }

        m.stopZK();
    }
}

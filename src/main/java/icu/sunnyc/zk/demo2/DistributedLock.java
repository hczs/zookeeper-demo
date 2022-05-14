package icu.sunnyc.zk.demo2;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * zk 原生 API 实现分布式锁 demo
 * @author ：hc
 * @date ：Created in 2022/5/14 13:13
 * @modified ：
 */
public class DistributedLock {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLock.class);

    /**
     * 分布式锁根节点路径
     */
    private final String rootNodePath = "/locks";

    /**
     * 分布式锁子节点路径
     */
    private final String subNodePath = rootNodePath + "/seq-";

    /**
     * 用于监控是否与 zk 服务端完成建立连接
     */
    private final CountDownLatch connectLatch = new CountDownLatch(1);

    /**
     * 用于监控前一个节点是否释放锁
     */
    private final CountDownLatch waitLatch = new CountDownLatch(1);

    /**
     * 监控的前一个节点的 path
     */
    private String waitPath = null;

    /**
     * 自己的专属锁路径
     */
    private String myLockPath;

    /**
     * zk 客户端连接对象
     */
    private final ZooKeeper zkClient;

    public DistributedLock(String connectString, int sessionTimeout) throws IOException, InterruptedException,
            KeeperException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                // 连接建立时 connectLatch 减一，唤醒后面 await 的内容，可以获取根节点状态做其他操作了
                if (event.getState() == KeeperState.SyncConnected) {
                    connectLatch.countDown();
                    logger.info("{} 成功与服务器建立连接!", Thread.currentThread().getName());
                }
                // 发生了 waitPath 删除事件，也就是 watch 的前一个节点人家用完资源释放锁了
                if (event.getType() == EventType.NodeDeleted && event.getPath().equals(waitPath)) {
                    logger.info("线程 {} 监控到锁 {} 已经被释放啦", Thread.currentThread().getName(), waitPath);
                    waitLatch.countDown();
                }
            }
        });
        // 阻塞直到与服务端完成建立连接
        connectLatch.await();
        // 获取根节点状态信息
        Stat rootNodeStat = zkClient.exists(rootNodePath, false);
        // 根节点不存在，就创建一个根节点，供后续使用
        if (rootNodeStat == null) {
            logger.warn("根节点为空，正在创建根节点...");
            String rootPath = zkClient.create(rootNodePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            logger.info("根节点创建完毕，根节点路径：{}", rootPath);
        }
    }

    /**
     * 获取分布式锁 （加锁）
     */
    public void getLock() {

        try {
            myLockPath = zkClient.create(subNodePath, null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            // 获取父节点下的所有子节点，看看自己是否是第一个
            List<String> subNodeList = zkClient.getChildren(rootNodePath, false);
            if (subNodeList.isEmpty()) {
                logger.error("未知异常，获取到子节点为空");
                return;
            }
            String myNodeName = myLockPath.split("/")[2];
            // 排序
            Collections.sort(subNodeList);
            // 第一个是自己，代表已经成功获取到锁
            if (myNodeName.equals(subNodeList.get(0))) {
                return;
            }
            // 第一个不是自己，没能成功获取锁，找到自己的前一名，然后 watch 它，直到它被释放，自己就顺利接手锁
            for (int i = 0; i < subNodeList.size(); i++) {
                if (myNodeName.equals(subNodeList.get(i))) {
                    // watch 前一个节点路径
                    waitPath = rootNodePath + "/" + subNodeList.get(i - 1);
                    // 在 waitPath 上注册监听器，当 waitPath 被删除时，zookeeper 会回调监听器的 process 方法
                    zkClient.getData(waitPath, true, new Stat());
                    logger.info("线程：{}，当前节点：{}，已监控前一个节点：{}", Thread.currentThread().getName(),
                            myNodeName, waitPath);
                }
            }
            // 上述已经 watch 了前一个锁了，此时只需要慢慢等待 waitPath 被释放就行
            waitLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }

    /**
     * 释放分布式锁 （解锁）
     */
    public void releaseLock() {
        // version -1 代表不管 version 多少，直接删除这个节点
        try {
            logger.info("线程：{} 已经释放锁 {}", Thread.currentThread().getName(), myLockPath);
            zkClient.delete(myLockPath, -1);
        } catch (InterruptedException e) {
            e.printStackTrace();
            Thread.currentThread().interrupt();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }
}

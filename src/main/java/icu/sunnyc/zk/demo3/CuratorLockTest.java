package icu.sunnyc.zk.demo3;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.retry.RetryNTimes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 使用 Curator 框架的分布式锁
 * @author ：hc
 * @date ：Created in 2022/5/14 21:33
 * @modified ：
 */
public class CuratorLockTest {

    private static final Logger logger = LoggerFactory.getLogger(CuratorLockTest.class);

    public static void main(String[] args) throws InterruptedException {
        String rootNodePath = "/locks";
        Random random = new Random();
        // 并发拿锁测试
        // 客户端数量
        int threadNumber = 5;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch end = new CountDownLatch(threadNumber);
        ExecutorService threadPool = Executors.newFixedThreadPool(threadNumber);
        for (int i = 0; i < threadNumber; i++) {
            threadPool.submit(() -> {
                try {
                    // 阻塞住 让这个线程别跑
                    start.await();
                    InterProcessMutex lock = new InterProcessMutex(getCuratorFramework(), rootNodePath);
                    // 获取锁
                    lock.acquire();
                    logger.info("{} 已经成功拿到锁，正在处理自己的事情", Thread.currentThread().getName());
                    Thread.sleep(random.nextInt(5000));
                    // 释放锁
                    lock.release();
                    logger.info("{} 已经释放锁", Thread.currentThread().getName());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    end.countDown();
                }
            });
        }
        start.countDown();
        logger.info("程序已启动");
        end.await();
        threadPool.shutdown();

    }

    private static CuratorFramework getCuratorFramework() {
        String zkServerPath = "hadoop102:2181,hadoop103:2181,hadoop104:2181";
        // 重试策略 重试 3 次，每次间隔 5 秒
        RetryNTimes retryPolicy = new RetryNTimes(3, 5000);
        CuratorFramework zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                // 连接创建超时时间
                .connectionTimeoutMs(2000)
                // 会话超时时间
                .sessionTimeoutMs(2000)
                .retryPolicy(retryPolicy).build();
        zkClient.start();
        logger.info("线程{} 连接已建立", Thread.currentThread().getName());
        return zkClient;
    }
}

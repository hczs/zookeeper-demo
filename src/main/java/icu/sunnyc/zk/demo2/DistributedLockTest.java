package icu.sunnyc.zk.demo2;

import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 分布式锁 测试
 * @author ：hc
 * @date ：Created in 2022/5/14 14:13
 * @modified ：
 */
public class DistributedLockTest {

    private static final Logger logger = LoggerFactory.getLogger(DistributedLockTest.class);

    public static void main(String[] args) throws InterruptedException {
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
                   // 获取锁
                   DistributedLock distributedLock = new DistributedLock("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000);
                   distributedLock.getLock();
                   // sleep 一个随机数 做业务处理
                   logger.info("{} 已经成功拿到锁，正在处理自己的事情", Thread.currentThread().getName());
                   Thread.sleep(random.nextInt(5000));
                   distributedLock.releaseLock();
               } catch (InterruptedException e) {
                   e.printStackTrace();
                   Thread.currentThread().interrupt();
               } catch (IOException | KeeperException e) {
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
}

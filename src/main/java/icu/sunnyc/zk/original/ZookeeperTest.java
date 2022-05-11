package icu.sunnyc.zk.original;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 使用 org.apache.zookeeper 提供的 API
 * @author ：hc
 * @date ：Created in 2022/5/9 21:20
 * @modified ：
 */
public class ZookeeperTest {

    private static String connectString = "hadoop102:2181,hadoop103:2181,hadoop104:2181";

    private static int sessionTimeout = 2000;

    private ZooKeeper zkClient = null;

    @Before
    public void init() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
//                System.out.println("------------------watcher------------------");
//                // 收到事件通知后的回调函数 （自己的业务逻辑）
//                System.out.println(event.getType() + "--" + event.getPath());
//                // 再次启动监听
//                try {
//                    List<String> children = zkClient.getChildren("/", true);
//                    for (String child : children) {
//                        System.out.println(child);
//                    }
//                } catch (Exception e) {
//                    e.printStackTrace();
//                }
//                System.out.println("------------------watcher------------------");
            }
        });
    }

    /**
     * 创建节点
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void createNode() throws InterruptedException, KeeperException {
        // 参数1：要创建的节点path 参数2：节点数据data 参数3：节点权限 参数4：节点类型（永久 临时 序列...）
        String result = zkClient.create("/javaapi", "dashuaige".getBytes(StandardCharsets.UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        System.out.println(result);
    }

    /**
     * 获取节点
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void getChildren() throws InterruptedException, KeeperException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }
        Thread.sleep(Integer.MAX_VALUE);
    }

    /**
     * 判断节点是否存在
     * @throws InterruptedException
     * @throws KeeperException
     */
    @Test
    public void exist() throws InterruptedException, KeeperException {
        Stat exists = zkClient.exists("/javaapi", true);
        System.out.println(exists == null ? "not exists" : "exists");
    }

}

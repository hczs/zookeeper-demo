package icu.sunnyc.zk.demo1;

import org.apache.zookeeper.*;
import org.apache.zookeeper.ZooDefs.Ids;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * 服务器动态上下线案例 服务端
 * @author ：hc
 * @date ：Created in 2022/5/10 21:17
 * @modified ：
 */
public class DistributeServer {

    private ZooKeeper zkClient;

    private static final String REGISTER_PATH = "/servers";

    /**
     * 服务注册
     */
    private void registerServer(String hostName) throws InterruptedException, KeeperException {
        String create = this.zkClient.create(REGISTER_PATH + "/" + hostName, hostName.getBytes(StandardCharsets.UTF_8),
                Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println(hostName + "is online " + create);
    }

    /**
     * 初始化 zk 客户端连接
     */
    private void initConnect(String connectString, int sessionTimeout) throws IOException {
        this.zkClient =  new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // 创建 zk 客户端连接
        DistributeServer distributeServer = new DistributeServer();
        distributeServer.initConnect("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000);
        // 把这个服务器注册进去
        distributeServer.registerServer("hadoop102");
        // 业务处理
        Thread.sleep(Long.MAX_VALUE);
    }
}

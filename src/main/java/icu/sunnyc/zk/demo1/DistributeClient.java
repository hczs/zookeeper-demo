package icu.sunnyc.zk.demo1;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 服务器动态上下线案例 客户端
 * @author ：hc
 * @date ：Created in 2022/5/10 21:30
 * @modified ：
 */
public class DistributeClient {

    private ZooKeeper zkClient;

    private static final String REGISTER_PATH = "/servers";

    /**
     * 初始化 zk 客户端连接
     */
    private void initConnect(String connectString, int sessionTimeout) throws IOException {
        this.zkClient =  new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    getServerList();
                } catch (InterruptedException | KeeperException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
            }
        });
    }

    /**
     * 打印获取到的服务器列表
     */
    private void getServerList() throws InterruptedException, KeeperException {
        List<String> children = this.zkClient.getChildren(REGISTER_PATH, true);
        ArrayList<String> servers = new ArrayList<>();
        for (String child : children) {
            byte[] data = this.zkClient.getData(REGISTER_PATH + "/" + child, false, null);
            servers.add(new String(data));
        }
        System.out.println(servers);
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        // 初始化 zk 客户端连接
        DistributeClient distributeClient = new DistributeClient();
        distributeClient.initConnect("hadoop102:2181,hadoop103:2181,hadoop104:2181", 2000);
        // 获取 servers 子节点的信息，获取服务器信息列表
        distributeClient.getServerList();
        // 业务处理
        Thread.sleep(Long.MAX_VALUE);
    }

}

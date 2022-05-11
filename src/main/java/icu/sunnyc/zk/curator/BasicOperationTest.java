package icu.sunnyc.zk.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.imps.CuratorFrameworkState;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * curator 框架使用
 * 创建客户端实例（curator 的重试策略）
 * 节点增删改查
 * 节点监听（一次性监听、永久监听、子节点监听）
 * @author ：hc
 * @date ：Created in 2022/5/10 21:58
 * @modified ：
 */
public class BasicOperationTest {

    private CuratorFramework zkClient = null;

    private static final String zkServerPath = "hadoop102:2181,hadoop103:2181,hadoop104:2181";

    private static final String nodePath = "/houge/dashuaige";

    @Before
    public void initConnection() {
        // 重试策略 重试 3 次，每次间隔 5 秒
        RetryNTimes retryPolicy = new RetryNTimes(3, 5000);
        zkClient = CuratorFrameworkFactory.builder()
                .connectString(zkServerPath)
                .sessionTimeoutMs(2000)
                .retryPolicy(retryPolicy)
                // 指定命名空间后，客户端所有操作都会以 /curator 开头
                .namespace("curator").build();
        zkClient.start();
    }

    /**
     * 获取服务状态
     */
    @Test
    public void getStatus() {
        CuratorFrameworkState state = zkClient.getState();
        System.out.println("服务当前状态：" + state);
        System.out.println("服务是否已经启动：" + (state == CuratorFrameworkState.STARTED));
    }

    /**
     * 创建节点
     */
    @Test
    public void createNode() throws Exception {
        byte[] data = "hahaha".getBytes(StandardCharsets.UTF_8);
        zkClient.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .withACL(Ids.OPEN_ACL_UNSAFE)
                .forPath(nodePath, data);
    }

    /**
     * 获取节点信息
     */
    @Test
    public void getNodeStat() throws Exception {
        Stat stat = new Stat();
        byte[] data = zkClient.getData().storingStatIn(stat).forPath(nodePath);
        System.out.println("节点数据：" + new String(data));
        System.out.println("节点信息：" + stat);
    }

    /**
     * 获取子节点列表
     */
    @Test
    public void getChildrenNodes() throws Exception {
        List<String> childrenNodes = zkClient.getChildren().forPath("/");
        for (String childrenNode : childrenNodes) {
            System.out.println(childrenNode);
        }
    }

    /**
     * 更新节点
     * 更新时可以传入版本号也可以不传入，如果传入则类似于乐观锁机制，只有在版本号正确的时候才会被更新
     */
    @Test
    public void updateNode() throws Exception {
        byte[] data = "xixixi".getBytes(StandardCharsets.UTF_8);
        // 传入版本号，如果版本号错误则拒绝更新操作,并抛出 BadVersion 异常 不信可以连续更新两次，到第二次 version 肯定不是 0 了 会抛异常
        zkClient.setData().withVersion(0).forPath(nodePath, data);
    }

    /**
     * 删除节点
     */
    @Test
    public void deleteNode() throws Exception {
        zkClient.delete()
                // 如果删除失败，就会继续执行直到删除成功
                .guaranteed()
                // 如果有子节点 就递归删除
                .deletingChildrenIfNeeded()
                // 传入版本号 如果版本号不对就不能删除 并抛出 BadVersion 异常
                .withVersion(1)
                .forPath(nodePath);
    }

    /**
     * 判断节点是否存在
     */
    @Test
    public void existsNode() throws Exception {
        // 如果节点存在就返回状态信息 如果不存在返回 null
        Stat stat = zkClient.checkExists().forPath(nodePath);
        System.out.println(nodePath + " 节点是否存在：" + (stat != null));
    }

    /**
     * 节点一次性监听 就和命令行的 -w 一个效果
     */
    @Test
    public void disposableWatch() throws Exception {
        zkClient.getData().usingWatcher(new CuratorWatcher() {
            @Override
            public void process(WatchedEvent event) {
                System.out.println("节点 " + event.getPath() + " 发生了事件:" + event.getType());
            }
        }).forPath(nodePath);
        // 休眠 测试观察效果 可以对节点进行多次操作 可以发现只会监听一次
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 创建永久监听
     */
    @Test
    public void permanentWatch() throws Exception {
        // 使用 NodeCache 包装节点，对其注册的监听作用于节点，且是永久性的
        // https://curator.apache.org/apidocs/deprecated-list.html
        // curator5 会弃用NodeCache
        // org.apache.curator.framework.recipes.cache.NodeCache 已经过时了 replace by CuratorCache
        NodeCache nodeCache = new NodeCache(zkClient, nodePath);
        // 通常设置为 true, 代表创建 nodeCache 时,就去获取对应节点的值并缓存
        nodeCache.start(true);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                ChildData currentData = nodeCache.getCurrentData();
                if (currentData != null) {
                    System.out.println("节点路径：" + currentData.getPath() + "数据：" + new String(currentData.getData()));
                }
            }
        });
        // 休眠观察效果
        Thread.sleep(Long.MAX_VALUE);
    }

    /**
     * 永久监听 /curator 下的所有子节点
     */
    @Test
    public void permanentChildrenNodesWatch() throws Exception {
        // 第三个参数是是否缓存节点内容
        PathChildrenCache pathChildrenCache = new PathChildrenCache(zkClient, "/", true);
        // StartMode 初始化方式
        // NORMAL: 异步初始化
        // BUILD_INITIAL_CACHE: 同步初始化
        // POST_INITIALIZED_EVENT: 异步并通知,初始化之后会触发 INITIALIZED 事件
        pathChildrenCache.start(StartMode.POST_INITIALIZED_EVENT);
        List<ChildData> childDataList = pathChildrenCache.getCurrentData();
        System.out.println("当前数据节点的子节点列表大小：" + childDataList.size());
        System.out.println("当前数据节点的子节点列表内容：");
        childDataList.forEach(childData -> System.out.println(childData.getPath()));

        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                switch (event.getType()) {
                    case INITIALIZED:
                        System.out.println("childrenCache 初始化完成");
                        List<ChildData> childDataList = pathChildrenCache.getCurrentData();
                        System.out.println("当前数据节点的子节点列表大小：" + childDataList.size());
                        System.out.println("当前数据节点的子节点列表内容：");
                        childDataList.forEach(childData -> System.out.println(childData.getPath()));
                        break;
                    case CHILD_ADDED:
                        System.out.println("增加子节点：" + event.getData().getPath());
                        break;
                    case CHILD_REMOVED:
                        System.out.println("删除子节点：" + event.getData().getPath());
                        break;
                    case CHILD_UPDATED:
                        System.out.println("被修改的子节点路径：" + event.getData().getPath());
                        System.out.println("修改后的子节点的值：" + new String(event.getData().getData()));
                        break;
                    default:
                        System.out.println("未知事件：" + event.getType());
                }
            }
        });
        // 休眠 用于测试
        Thread.sleep(Long.MAX_VALUE);
    }

    @After
    public void destroy() {
        if (zkClient != null) {
            zkClient.close();
        }
    }

}

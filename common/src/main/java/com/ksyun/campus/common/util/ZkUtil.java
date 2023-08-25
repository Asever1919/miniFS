package com.ksyun.campus.common.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

import java.util.List;

@Slf4j
public class ZkUtil {

    private final CuratorFramework client;

    protected final ObjectMapper mapper = new ObjectMapper();

    public ZkUtil(CuratorFramework client) {
        this.client = client;
    }

    /**
     * 创建节点无数据
     */
    public void createNode(String path, boolean isEphemeral) throws Exception {
        // 创建临时节点
        String node = client.create()
                .creatingParentsIfNeeded()
                .withMode(isEphemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
                .forPath(path);
        log.info("create node: {}", node);
    }

    /**
     * 创建节点有数据
     */
    public String createNode(String path, Object value, boolean isEphemeral) throws Exception {
        byte[] bytes = mapper.writeValueAsBytes(value);
        String node = client.create()
                .creatingParentsIfNeeded()
                .withMode(isEphemeral ? CreateMode.EPHEMERAL : CreateMode.PERSISTENT)
                .forPath(path, bytes);
        log.info("create node: {}, value: {}", node, value);
        return node;
    }

    /**
     * 删除节点信息
     */
    public void deleteNode(String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        if (stat == null) {
            throw new RuntimeException("文件不存在");
        }
        client.delete()
                .guaranteed() // 保障机制，若未删除成功，只要会话有效会在后台一直尝试删除
//                .deletingChildrenIfNeeded() // 若当前节点包含子节点，子节点也删除
                .forPath(path);
        log.info("{} is deleted", path);
    }

    /**
     * 查询子节点
     */
    public List<String> getChildren(String path) throws Exception {
        return client.getChildren().forPath(path);
    }

    /**
     * 获取节点存储的值
     */
    public String getNodeData(String path) throws Exception {
//        Stat stat = new Stat();
        byte[] bytes = client.getData().forPath(path);
//        String data = new String(bytes);
//        log.info("{} data is : {}", path, data);
//        log.info("current stat version is {}, createTime is {}", stat.getVersion(), stat.getCtime());
        return new String(bytes);
    }

    public Stat checkExists(String path) throws Exception {
        if (StringUtils.isEmpty(path)) {
            return null;
        }
        return client.checkExists().forPath(path);
    }

    /**
     * 设置节点数据
     */
    public void setNodeData(String path, Object value) throws Exception {
        byte[] bytes = mapper.writeValueAsBytes(value);
        Stat stat = client.checkExists().forPath(path);
        if (null == stat) {
            log.info("{} Znode is not exists", path);
            throw new RuntimeException(String.format("%s Znode is not exists", path));
        }
        String nodeData = getNodeData(path);
        client.setData().withVersion(stat.getVersion()).forPath(path, bytes);
//        log.info("{} Znode data is set. old vaule is {}, new data is {}", path, nodeData, value);
    }

    /**
     * 创建给定节点的监听事件  监听一个节点的更新和创建事件(不包括删除)
     */
    public void addWatcherWithNodeCache(String path) throws Exception {
        // dataIsCompressed if true, data in the path is compressed
        NodeCache nodeCache = new NodeCache(client, path, false);
        NodeCacheListener listener = () -> {
            ChildData currentData = nodeCache.getCurrentData();
            log.info("{} Znode data is chagnge,new data is ---  {}", currentData.getPath(), new String(currentData.getData()));
        };
        nodeCache.getListenable().addListener(listener);
        nodeCache.start();
    }

    /**
     * 监听给定节点下的子节点的创建、删除、更新
     */
    public void addWatcherWithChildCache(String path, PathChildrenCacheListener listener) throws Exception {
        //cacheData if true, node contents are cached in addition to the stat
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client, path, false);
        pathChildrenCache.getListenable().addListener(listener);
        // StartMode : NORMAL  BUILD_INITIAL_CACHE  POST_INITIALIZED_EVENT
        // NORMAL:异步初始化, BUILD_INITIAL_CACHE:同步初始化, POST_INITIALIZED_EVENT:异步初始化,初始化之后会触发事件
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);
    }

    /**
     * 监听给定节点的创建、更新（不包括删除） 以及 该节点下的子节点的创建、删除、更新动作。
     */
    public void addWatcherWithTreeCache(String path, TreeCacheListener listener) throws Exception {
        TreeCache treeCache = new TreeCache(client, path);
        treeCache.getListenable().addListener(listener);
        treeCache.start();
    }

}

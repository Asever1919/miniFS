package com.ksyun.campus.client.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ZkConnectUtil {

    private static final String connectString = System.getProperty("zookeeper.addr", "127.0.0.1:2181");

    private static final int sessionTimeout = 2000;

    // zk client 重试间隔时间
    private static final int baseSleepTimeMs = 5000;

    //zk client 重试次数
    private static final int retryCount = 5;

    private static ClusterInfo.MetaServerMsg metaServerMsg;

    private static final ZkUtil zkUtil = new ZkUtil(ZkConnectUtil.getClient());

    protected static final ObjectMapper mapper = new ObjectMapper();

    public static CuratorFramework getClient() {
        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, retryCount))
                .build();
        client.start();
        return client;
    }

    public static ClusterInfo getClusterInfo() throws Exception {
        List<String> metaChildren = zkUtil.getChildren(ServerConstant.META_SERVER_PATH);
        List<String> dataChildren = zkUtil.getChildren(ServerConstant.DATA_SERVER_PATH);
        ClusterInfo clusterInfo = new ClusterInfo();
        List<ClusterInfo.MetaServerMsg> slaveList = new ArrayList<>();
        for (int i = 0; i < metaChildren.size(); i++) {
            String child = metaChildren.get(i);
            String nodeData = zkUtil.getNodeData(ServerConstant.META_SERVER_PATH + "/" + child);
            ClusterInfo.MetaServerMsg metaServerMsg = mapper.readValue(nodeData, ClusterInfo.MetaServerMsg.class);
            if (i == 0) {
                clusterInfo.setMasterMetaServer(metaServerMsg);
            } else {
                slaveList.add(metaServerMsg);
            }
        }
        clusterInfo.setSlaveMetaServer(slaveList);
        List<ClusterInfo.DataServerMsg> dataServerMsgList = new ArrayList<>();
        for (String child : dataChildren) {
            String nodeData = zkUtil.getNodeData(ServerConstant.DATA_SERVER_PATH + "/" + child);
            ClusterInfo.DataServerMsg dataServerMsg = mapper.readValue(nodeData, ClusterInfo.DataServerMsg.class);
            dataServerMsgList.add(dataServerMsg);
        }
        clusterInfo.setDataServer(dataServerMsgList);
        return clusterInfo;
    }

    public static String getMetaServerUrl() throws Exception {
        if (metaServerMsg == null) {
            refreshServerInfo();
            if (metaServerMsg == null) {
                throw new RuntimeException("metaServer服务已下线");
            }
        }
        return "http://" + metaServerMsg.getHost() + ":" + metaServerMsg.getPort();
    }

    public static void refreshServerInfo() throws Exception {
        log.info("刷新服务");
        metaServerMsg = getClusterInfo().getMasterMetaServer();
    }
}

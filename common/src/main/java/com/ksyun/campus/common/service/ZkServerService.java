package com.ksyun.campus.common.service;

import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.curator.framework.CuratorFramework;

import java.io.File;
import java.util.Objects;

@Slf4j
public class ZkServerService extends ZkUtil {

    private final ClusterInfo.ServerMsg serverMsg;
    private final String serviceName;
    private final String basePath = "/servers";
    private final String path; // zk中的路径

    public ZkServerService(CuratorFramework client, ClusterInfo.ServerMsg serverMsg, String serviceName) {
        super(client);
        this.serviceName = serviceName;
        this.serverMsg = serverMsg;
        this.path = basePath + "/" + serviceName + "/" + serverMsg.getHost() + ":" + serverMsg.getPort();
    }

    public void register() {
        // 服务名称
        String serviceNamePath = basePath + "/" + serviceName;
        try {
            if (checkExists(serviceNamePath) == null) {
                // 创建持久化的节点，作为服务名称
                createNode(serviceNamePath, false);
            }
            String urlNode = createNode(path, serverMsg, true);
            log.info("服务: {} 成功注册到zookeeper server", urlNode);
        } catch (Exception e) {
            log.error("服务注册失败", e);
        }
    }

    /**
     *
     * @param isAddFile 是否为添加文件
     * @param baseDir 数据存储目录
     */
    public void updateDataServerInfo(boolean isAddFile, File baseDir) {
        // 服务名称
        try {
            String nodeData = getNodeData(path);
            ClusterInfo.DataServerMsg dataServerMsg = mapper.readValue(nodeData, ClusterInfo.DataServerMsg.class);
            if (isAddFile) {
                dataServerMsg.setFileTotal(dataServerMsg.getFileTotal() + 1);
            }
            dataServerMsg.setUseCapacity(FileUtils.sizeOfDirectory(baseDir));
            dataServerMsg.setFileTotal(Objects.requireNonNull(baseDir.listFiles()).length);
            setNodeData(path, dataServerMsg);
        } catch (Exception e) {
            log.error("更新DataServer失败", e);
        }
    }
}

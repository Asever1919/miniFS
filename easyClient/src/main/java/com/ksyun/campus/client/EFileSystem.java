package com.ksyun.campus.client;


import com.fasterxml.jackson.core.type.TypeReference;
import com.ksyun.campus.client.util.ZkConnectUtil;
import com.ksyun.campus.common.consts.ClientConstant;
import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import com.ksyun.campus.common.response.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.hc.client5.http.HttpHostConnectException;


import java.io.File;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class EFileSystem extends FileSystem {

    public EFileSystem() {
        File file = new File(ClientConstant.TMP_PATH.toUri());
        log.info("tmp path is: {}", file);
        if (!file.exists()) {
            file.mkdirs();
        }
        fileSystem = "default";
    }

    public EFileSystem(String fileName) {
        File file = new File(ClientConstant.TMP_PATH.toUri());
        log.info("tmp path is: {}", file);
        if (!file.exists()) {
            file.mkdirs();
        }
        if (StringUtils.isEmpty(fileName)) {
            fileName = "default";
        }
        fileSystem = fileName; // fileName就是FileSystem中的fileSystem
    }

    public FSInputStream open(String path) throws Exception {
        StatInfo fileStats = getFileStats(path);
        if (fileStats == null) {
            throw new RuntimeException("文件不存在");
        }
        return new FSInputStream(fileStats.getReplicaData(), fileStats);
    }

    public FSOutputStream create(String path) throws Exception {
        List<ReplicaData> replicaDataList = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            // 请求metaServer 创建文件
            try {
                replicaDataList = new ArrayList<>();
                ApiResponse response = callMetaServerByPath(ServerConstant.CREATE_URL, path);
                int code = response.getCode();
                if (code == 200) {
                    replicaDataList = mapper.convertValue(response.getData(), new TypeReference<List<ReplicaData>>() {});
                    log.info("创建文件: {} 成功", path);
                } else {
                    log.info("创建文件: {} 失败", path);
                }
                break;
            } catch (HttpHostConnectException e) {
                ZkConnectUtil.refreshServerInfo();
            }
        }
        // 传入要上传的三副本信息
        return new FSOutputStream(replicaDataList, path, fileSystem);
    }

    // mkdir
    public boolean mkdir(String path) throws Exception {
        for (int i = 0; i < 5; i++) {
            try {
                ApiResponse response = callMetaServerByPath(ServerConstant.MKDIR_URL, path);
                int code = response.getCode();
                if (code == 200) {
                    log.info("创建目录: {} 成功", path);
                } else {
                    log.info("创建目录: {} 失败", path);
                }
                return code == 200;
            } catch (HttpHostConnectException e) {
                ZkConnectUtil.refreshServerInfo();
            }
        }
        return false;
    }

    // delete
    public boolean delete(String path) throws Exception {
        for (int i = 0; i < 5; i++) {
            try {
                ApiResponse response = callMetaServerByPath(ServerConstant.DELETE_URL, path);
                int code = response.getCode();
                if (code == 200) {
                    log.info("删除目录或文件: {} 成功", path);
                } else {
                    log.warn("删除目录或文件: {} 失败", path);
                }
                return code == 200;
            } catch (HttpHostConnectException e) {
                ZkConnectUtil.refreshServerInfo();
            }
        }
        return false;
    }

    // stats
    public StatInfo getFileStats(String path) throws Exception {
        for (int i = 0; i < 5; i++) {
            try {
                ApiResponse response = callMetaServerByPath(ServerConstant.STATS_URL, path);
                int code = response.getCode();
                if (code == 200) {
                    return mapper.convertValue(response.getData(), StatInfo.class);
                } else {
                    return null;
                }
            } catch (HttpHostConnectException e) {
                ZkConnectUtil.refreshServerInfo();
            }
        }
        return null;
    }

    // listdir
    public List<StatInfo> listFileStats(String path) throws Exception {
        for (int i = 0; i < 5; i++) {
            try {
                ApiResponse response = callMetaServerByPath(ServerConstant.LISTDIR_URL, path);
                int code = response.getCode();
                if (code == 200) {
                    return mapper.convertValue(response.getData(), new TypeReference<List<StatInfo>>() {});
                } else {
                    return null;
                }
            } catch (HttpHostConnectException e) {
                ZkConnectUtil.refreshServerInfo();
            }
        }
        return null;
    }

    /**
     * 获取集群信息
     */
    public ClusterInfo getClusterInfo() throws Exception {
        return ZkConnectUtil.getClusterInfo();
    }
}

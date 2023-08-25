package com.ksyun.campus.metaserver.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.domain.FileType;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import com.ksyun.campus.common.dto.CommitWriteDTO;
import com.ksyun.campus.common.response.ApiResponse;
import com.ksyun.campus.common.util.ZkUtil;
import com.ksyun.campus.metaserver.raft.rpc.RpcClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MetaService {

    private final ZkUtil zkUtil;

    public static final String baseFilePath = "/fileSystem";

    private final ObjectMapper mapper;

    private final RestTemplate restTemplate;

    public MetaService(ZkUtil zkUtil, ObjectMapper mapper, RestTemplate restTemplate) {
        this.zkUtil = zkUtil;
        this.mapper = mapper;
        this.restTemplate = restTemplate;
    }

    private String getZkPath(String fileSystem, String path) {
        Path p = Paths.get(baseFilePath, fileSystem, path);
        return p.toString();
    }

    public StatInfo getFileStats(String fileSystem, String path) {
        String zkPath = getZkPath(fileSystem, path);
        try {
            String nodeData = RpcClientUtil.getNodeData(zkPath);
            return mapper.readValue(nodeData, StatInfo.class);
        }
        catch (Exception e) {
            log.error("获取文件信息失败", e);
            throw new RuntimeException("获取文件信息失败");
        }
    }

    public List<StatInfo> listDir(String fileSystem, String path) {
        String zkPath = getZkPath(fileSystem, path);
        try {
            String s = RpcClientUtil.getChildren(zkPath);
            return mapper.readValue(s, new TypeReference<List<StatInfo>>() {});
        }
        catch (Exception e) {
            log.error("获取文件列表信息失败", e);
            throw new RuntimeException("获取文件列表信息失败");
        }
    }

    public void createDirOrFile(String fileSystem, String path, FileType fileType) {
        String zkPath = getZkPath(fileSystem, path);
        try {
            // 若已存在且为文件，先删除，实现覆盖写
            String isExists = RpcClientUtil.checkExists(zkPath);
            if (StringUtils.isNotEmpty(isExists)) {
                isExists = RpcClientUtil.checkExists(zkPath);
                if (StringUtils.isNotEmpty(isExists)) {
                    StatInfo statInfo = mapper.readValue(RpcClientUtil.getNodeData(zkPath), StatInfo.class);
                    if (statInfo.getType().equals(FileType.File) && fileType.equals(FileType.File)) {
                        RpcClientUtil.deleteNode(zkPath);
                    }
                }
            }
            isExists = RpcClientUtil.checkExists(zkPath);
            if (StringUtils.isNotEmpty(isExists)) {
                throw new RuntimeException("存在相同路径的文件夹，删除失败");
            }
            // 父级目录不存在时，递归创建
            int idx = path.lastIndexOf("/");
            if (idx != -1) {
                String fatherPath = path.substring(0, idx);
                isExists = RpcClientUtil.checkExists(getZkPath(fileSystem, fatherPath));
                if (StringUtils.isEmpty(isExists)) {
                    createDirOrFile(fileSystem, fatherPath, FileType.Directory);
                }
            }
            StatInfo statInfo = new StatInfo();
            statInfo.setPath(path);
            statInfo.setVolume(fileSystem);
            statInfo.setSize(0);
            statInfo.setMtime(System.currentTimeMillis());
            statInfo.setType(fileType);
            if (zkPath.equals(Paths.get(baseFilePath, fileSystem).toString())) {
                if (StringUtils.isEmpty(RpcClientUtil.checkExists("fileSystem"))) {
                    RpcClientUtil.createNode("fileSystem", null);
                }
                statInfo.setType(FileType.Volume);
            }
            RpcClientUtil.createNode(zkPath, statInfo);
            log.info("创建文件夹/文件: {} 成功", path);
        } catch (Exception e) {
            log.error("创建文件夹/文件失败", e);
            throw new RuntimeException("创建文件夹/文件失败");
        }
    }

    public void delete(String fileSystem, String path) {
        String zkPath = getZkPath(fileSystem, path);
        try {
            String nodeData = RpcClientUtil.getNodeData(zkPath);
            StatInfo statInfo = mapper.readValue(nodeData, StatInfo.class);
            if (statInfo.getType().equals(FileType.File) && statInfo.getReplicaData() != null) {
                for (ReplicaData replicaData : statInfo.getReplicaData()) {
                    String url = "http://" + replicaData.getDsNode() + ServerConstant.DELETE_LOCAL_URL;
                    MultiValueMap<String,Object> params = new LinkedMultiValueMap<>();
                    params.add("path", replicaData.getPath());
                    HttpHeaders headers = new HttpHeaders();
                    headers.setContentType(MediaType.MULTIPART_FORM_DATA);
                    HttpEntity<MultiValueMap<String,Object>> entity  = new HttpEntity<>(params, headers);
                    ApiResponse apiResponse = restTemplate.postForObject(url, entity, ApiResponse.class);
                }
            }
            RpcClientUtil.deleteNode(zkPath);
            log.info("删除文件夹/文件: {} 成功", path);
        } catch (Exception e) {
            log.error("删除文件夹/文件失败", e);
            throw new RuntimeException("删除文件夹/文件失败");
        }
    }

    // 选择空间剩余最多的节点，并排除某些节点
    public List<ReplicaData> pickDataServer(int count, Set<String> excludeDsNode, ReplicaData existReplicaData) {
        try {
            List<String> dataChildren = zkUtil.getChildren(ServerConstant.DATA_SERVER_PATH);
            List<ClusterInfo.DataServerMsg> dataServerMsgList = new ArrayList<>();
            for (String child : dataChildren) {
                String nodeData = zkUtil.getNodeData(ServerConstant.DATA_SERVER_PATH + "/" + child);
                ClusterInfo.DataServerMsg dataServerMsg = mapper.readValue(nodeData, ClusterInfo.DataServerMsg.class);
                dataServerMsgList.add(dataServerMsg);
            }
            List<ClusterInfo.DataServerMsg> collect = dataServerMsgList.stream()
                    .filter(e -> {
                        String dsNode = e.getHost() + ":" + e.getPort();
                        return !excludeDsNode.contains(dsNode);
                    })
                    .sorted((a, b) -> {
                        double aPercent = (double) a.getUseCapacity() / a.getCapacity();
                        double bPercent = (double) b.getUseCapacity() / b.getCapacity();
                        return Double.compare(aPercent, bPercent);
                    })
                    .limit(count)
                    .collect(Collectors.toList());
            List<ReplicaData> replicaDataList = new ArrayList<>();
            int idx = Objects.nonNull(existReplicaData) ? Integer.parseInt(existReplicaData.getId()) + 1 : 0;
            String path = Objects.nonNull(existReplicaData) ? existReplicaData.getPath() : String.valueOf(UUID.randomUUID());
            for (int i = 0; i < collect.size(); i++) {
                ClusterInfo.DataServerMsg dataServerMsg = collect.get(i);
                ReplicaData replicaData = new ReplicaData();
                replicaData.setId(String.valueOf(idx ++ ));
                replicaData.setDsNode(dataServerMsg.getHost() + ":" + dataServerMsg.getPort());
                replicaData.setPath(path);
                replicaDataList.add(replicaData);
            }
            return replicaDataList;
        } catch (Exception e) {
            log.error("查询dataServer失败", e);
            throw new RuntimeException("查询dataServer失败");
        }
    }

    public void commitWrite(String fileSystem, CommitWriteDTO commitWriteDTO) {
        String zkPath = getZkPath(fileSystem, commitWriteDTO.getPath());
        try {
            String nodeData = RpcClientUtil.getNodeData(zkPath);
            StatInfo statInfo = mapper.readValue(nodeData, StatInfo.class);
            statInfo.setSize(commitWriteDTO.getFileSize());
            statInfo.setReplicaData(commitWriteDTO.getReplicaDataList());
            statInfo.setMtime(System.currentTimeMillis());
            RpcClientUtil.setNodeData(zkPath, statInfo);
            log.info("提交写:{} 成功", commitWriteDTO.getPath());
        } catch (Exception e) {
            log.error("提交写失败", e);
            throw new RuntimeException("提交写失败");
        }
    }
}

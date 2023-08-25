package com.ksyun.campus.metaserver.services;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.FileType;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import com.ksyun.campus.common.response.ApiResponse;
import com.ksyun.campus.common.util.ZkUtil;
import com.ksyun.campus.metaserver.raft.dataobject.FileMeta;
import com.ksyun.campus.metaserver.raft.dataobject.FileMetaUtil;
import com.ksyun.campus.metaserver.raft.rpc.RpcClientUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientException;
import org.springframework.web.client.RestTemplate;

import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class FsckServices {

    private final ObjectMapper mapper;

    private List<StatInfo> allFileList;

    private final RestTemplate restTemplate;

    private final MetaService metaService;

    private FileMeta fileMeta;

    public FsckServices(ObjectMapper mapper, RestTemplate restTemplate, MetaService metaService) {
        this.mapper = mapper;
        this.restTemplate = restTemplate;
        this.metaService = metaService;
    }

    @Scheduled(fixedRate = 20 * 1000L, initialDelay = 20 * 1000L) // 每隔 30 分钟执行一次
    public void fsckTask() throws Exception {
        allFileList = new ArrayList<>();
        String allMeta = RpcClientUtil.getAllMeta(MetaService.baseFilePath);
        fileMeta = mapper.readValue(allMeta, FileMeta.class);
        getAllFileList(MetaService.baseFilePath);
        List<String> collect = allFileList.stream()
                .map(StatInfo::getPath)
                .collect(Collectors.toList());
        log.info("所有文件列表: {}", collect);
        // 先检查空节点，如果没恢复成功，不删除节点
        checkEmptyFile();
        checkFileReplica();
    }

    // 全量扫描文件列表
    private void getAllFileList(String path) throws Exception {
        FileMetaUtil fileMetaUtil = new FileMetaUtil(fileMeta, path, null);
        List<StatInfo> children = fileMetaUtil.getChildren();
        for (StatInfo statInfo : children) {
            String curPath = Paths.get(MetaService.baseFilePath, statInfo.getVolume(), statInfo.getPath()).toString();
            if (statInfo.getType().equals(FileType.File)) {
                allFileList.add(statInfo);
            } else if (statInfo.getType().equals(FileType.Volume) ||
                    statInfo.getType().equals(FileType.Directory)) {
                getAllFileList(curPath);
            }
        }
    }

    private void checkFileReplica() throws Exception {
        // 存储所有ds上的文件列表
        Map<String, List<StatInfo>> stringListMap = new HashMap<>();
        for (StatInfo statInfo : allFileList) {
            if (statInfo.getReplicaData() != null) {
                for (ReplicaData replicaData : statInfo.getReplicaData()) {
                    stringListMap.compute(replicaData.getDsNode(), (k, v) -> {
                        if (Objects.isNull(v)) {
                            v = new ArrayList<>();
                        }
                        v.add(statInfo);
                        return v;
                    });
                }
            }
        }
        // 失效的文件和ds列表映射
        Map<String, List<String>> invalidMap = new HashMap<>();
        // 向ds发送请求检测文件是否有效
        stringListMap.forEach((dataServerUrl, statInfoList) -> { // statInfoList待检测的文件
            List<StatInfo> invalidList = new ArrayList<>();
            try {
                String url = "http://" + dataServerUrl + ServerConstant.CHECK_URL;
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<List<StatInfo>> entity = new HttpEntity<>(statInfoList, headers);
                ApiResponse apiResponse = restTemplate.postForObject(url, entity, ApiResponse.class);
                if (apiResponse != null && apiResponse.getCode() == 200) {
                    invalidList = mapper.convertValue(apiResponse.getData(), new TypeReference<List<StatInfo>>() {
                    });
                }
            } catch (RestClientException e) { // 连接服务失败
                invalidList = statInfoList;
            } finally {
                invalidList.forEach(e -> invalidMap.compute(e.getZkPath(), (k, v) -> {
                    if (Objects.isNull(v)) {
                        v = new ArrayList<>();
                    }
                    v.add(dataServerUrl);
                    return v;
                }));
            }
        });
        for (StatInfo statInfo : allFileList) {
            Boolean isRecover = null; // 不为空表示尝试恢复
            if (invalidMap.containsKey(statInfo.getZkPath())) { // ds上不存在目标文件
                isRecover = recoverFile(statInfo, invalidMap.get(statInfo.getZkPath()));
            }
            // 检查文件副本数量
            else if (statInfo.getReplicaData() != null
                    && statInfo.getReplicaData().size() < 3
                    && statInfo.getReplicaData().size() > 0) {
                isRecover = recoverFile(statInfo, new ArrayList<>());
            }
            if (isRecover != null) {
                if (isRecover) {
                    // 恢复成功，更新文件副本
                    statInfo.setMtime(System.currentTimeMillis());
                    RpcClientUtil.setNodeData(statInfo.getZkPath(), statInfo);
                    log.info("文件:{} 恢复成功", statInfo.getZkPath());
                } else {
                    log.warn("文件:{} 恢复失败", statInfo.getZkPath());
                }
            }
        }
    }

    private boolean recoverFile(StatInfo statInfo, List<String> invalidDsList) throws Exception {
        Set<String> validDsNode = new HashSet<>(); // 有数据的节点
        List<ReplicaData> replicaDataList = statInfo.getReplicaData();
        for (int i = replicaDataList.size() - 1; i >= 0; i--) {
            String dsNode = replicaDataList.get(i).getDsNode();
            if (!invalidDsList.contains(dsNode)) {
                validDsNode.add(dsNode);
            } else {
                replicaDataList.remove(i);
            }
        }
        if (validDsNode.isEmpty()) {
            return false;
        }
        int needRecoverCount = 3 - validDsNode.size();
        List<ReplicaData> recoverReplicaDataList = metaService.pickDataServer(needRecoverCount, validDsNode,
                replicaDataList.get(replicaDataList.size() - 1));
        log.info("文件:{} 尝试恢复到ds列表: {}", statInfo.getZkPath(), recoverReplicaDataList);
        // 请求ds恢复
        for (ReplicaData replicaData : replicaDataList) {
            String url = "http://" + replicaData.getDsNode() + ServerConstant.RECOVER_URL;
            try {
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<List<ReplicaData>> entity = new HttpEntity<>(recoverReplicaDataList, headers);
                ApiResponse apiResponse = restTemplate.postForObject(url, entity, ApiResponse.class);
                if (apiResponse != null && apiResponse.getCode() == 200) {
                    replicaDataList.addAll(recoverReplicaDataList);
                    return true;
                }
            } catch (Exception e) {
                log.error("尝试请求:{} 失败", url);
            }
        }
        return false;
    }

    // 删除空引用
    private void checkEmptyFile() throws Exception {
        for (StatInfo statInfo : allFileList) {
            long pastTime = (System.currentTimeMillis() - statInfo.getMtime()) / 1000 / 60; // 超时时间，有可能正在传输
            // 无副本存储
            if ((statInfo.getReplicaData() == null || statInfo.getReplicaData().isEmpty()) && pastTime > 30) {
                String path = Paths.get(MetaService.baseFilePath, statInfo.getVolume(), statInfo.getPath()).toString();
                RpcClientUtil.deleteNode(path);
                log.warn("文件:{} 无引用，将删除", path);
            }
        }
    }
}

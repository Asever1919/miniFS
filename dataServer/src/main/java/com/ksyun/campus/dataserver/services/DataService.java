package com.ksyun.campus.dataserver.services;

import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import com.ksyun.campus.common.response.ApiResponse;
import com.ksyun.campus.common.service.ZkServerService;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

@Service
@Slf4j
public class DataService {

    private final Path baseDataPath;

    private final RestTemplate restTemplate;

    private final ZkServerService zkServerService;

    private final File baseDataFile;

    private final ClusterInfo.DataServerMsg dataServerMsg;

    public DataService(Path baseDataPath, RestTemplate restTemplate, ZkServerService zkServerService, ClusterInfo.DataServerMsg dataServerMsg) {
        this.baseDataPath = baseDataPath;
        this.restTemplate = restTemplate;
        this.zkServerService = zkServerService;
        this.baseDataFile = new File(baseDataPath.toString());
        this.dataServerMsg = dataServerMsg;
    }

    // 写本地
    public File saveLocalFile(String path, MultipartFile file) {
        if (!baseDataFile.exists()) {
            baseDataFile.mkdirs();
        }
        try {
            if (StringUtils.isNotEmpty(path)) {
                long size = file.getSize();
                if (this.baseDataFile.length() + size > this.dataServerMsg.getCapacity()) {
                    throw new RuntimeException("超出容量上限");
                }
                File localFile = new File(baseDataPath.toString(), path);
                localFile.createNewFile();
                file.transferTo(localFile);
                log.info("保存本地文件: {}, 成功", localFile.getPath());
                zkServerService.updateDataServerInfo(true, this.baseDataFile);
                return localFile;
            }
        } catch (Exception e) {
            log.error("保存本地文件失败", e);
        }
        return null;
    }

    // 调用远程ds服务写接口，同步副本，已达到多副本数量要求
    public boolean writeToOtherDs(String path, File file, List<ReplicaData> replicaDataList) {
        log.info("同步到其他DataServer:{} ", replicaDataList);
        if (Objects.isNull(file)) { // 存在本地文件
            file = new File(baseDataPath.toString(), path);
        }
        List<Thread> threads = new ArrayList<>();
        AtomicInteger count200 = new AtomicInteger(0);
        // 异步发送文件
        for (int i = 0; i < replicaDataList.size(); i++) {
            int finalI = i;
            File finalFile = file;
            Thread thread = new Thread(() -> {
                ReplicaData replicaData = replicaDataList.get(finalI);
                String dataServerUrl = "http://" + replicaData.getDsNode() + ServerConstant.WRITE_LOCAL_URL;
                MultiValueMap<String,Object> params = new LinkedMultiValueMap<>();
                FileSystemResource fileSystemResource = new FileSystemResource(finalFile.getPath());
                params.add("file", fileSystemResource);
                params.add("path", path);
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.MULTIPART_FORM_DATA);
                HttpEntity<MultiValueMap<String,Object>> entity  = new HttpEntity<>(params, headers);
                ApiResponse apiResponse = restTemplate.postForObject(dataServerUrl, entity, ApiResponse.class);
                if (apiResponse != null) {
                    if (apiResponse.getCode() != 200) {
                        log.warn("同步到:{} 失败, 原因: {}", dataServerUrl, apiResponse.getMsg());
                    } else {
                        count200.incrementAndGet();
                    }
                }
            });
            thread.start();
            threads.add(thread);
        }
        try {
            for (Thread thread : threads) {
                thread.join();
            }
            return count200.get() == replicaDataList.size();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public FileSystemResource read(String path) {
        File file = new File(baseDataPath.toString(), path);
        return new FileSystemResource(file);
    }

    @Scheduled(fixedDelay = 30 * 1000L, initialDelay = 30 * 1000L)
    public void uploadSelfInfo() {
        log.info("定时上报dataServer信息");
        zkServerService.updateDataServerInfo(false, baseDataFile);
    }

    public List<StatInfo> checkFile(List<StatInfo> statInfoList) {
        List<StatInfo> invalidStatInfoList = new ArrayList<>();
        try {
            // 合法文件集合
            Set<File> validFileSet = new HashSet<>();
            // 遍历所有待检测的文件
            for (StatInfo statInfo : statInfoList) {
                String path = statInfo.getReplicaData().get(0).getPath();
                File file = new File(baseDataFile, path);
                if (!file.exists() || file.length() != statInfo.getSize()) { // 不存在或大小不一致均不合法
                    invalidStatInfoList.add(statInfo);
                } else {
                    validFileSet.add(file);
                }
            }
            // 清除不合法的文件
            for (File file : Objects.requireNonNull(baseDataFile.listFiles())) {
                if (!validFileSet.contains(file)) {
                    file.delete();
                }
            }
        } catch (Exception e) {
            log.error("检测文件异常", e);
            // 异常时所有待检测不合法
            invalidStatInfoList = statInfoList;
        }
        return invalidStatInfoList;
    }

    public void deleteFile(String path) {
        File file = new File(baseDataFile, path);
        file.delete();
    }
}

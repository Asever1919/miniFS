package com.ksyun.campus.dataserver.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import com.ksyun.campus.common.response.ApiResponse;
import com.ksyun.campus.dataserver.services.DataService;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.IOException;
import java.util.List;

@RestController("/")
public class DataController {

    private final DataService dataService;

    private final ObjectMapper mapper;

    public DataController(DataService dataService, ObjectMapper mapper) {
        this.dataService = dataService;
        this.mapper = mapper;
    }

    /**
     * 1、读取request content内容并保存在本地磁盘下的文件内
     * 2、同步调用其他ds服务的write，完成另外2副本的写入
     * 3、返回写成功的结果及三副本的位置
     */
    @PostMapping("write")
    public ApiResponse writeFile(@RequestParam String path, @RequestParam MultipartFile file,
                                 @RequestParam String replicaDataListJson) throws JsonProcessingException {
        List<ReplicaData> replicaDataList = mapper.readValue(replicaDataListJson, new TypeReference<List<ReplicaData>>(){});
        File localFile = dataService.saveLocalFile(path, file);
        if (localFile == null) {
            return ApiResponse.failure("写入文件失败");
        }
        // replicaDataList第一个元素为自身
        boolean isSuccess = dataService.writeToOtherDs(path, localFile, replicaDataList.subList(1, replicaDataList.size()));
        return isSuccess ? ApiResponse.success() : ApiResponse.failure("同步副本失败");
    }

    @PostMapping("write-local")
    public ApiResponse writeLocalFile(@RequestParam String path, @RequestParam MultipartFile file) {
        dataService.saveLocalFile(path, file);
        return ApiResponse.success();
    }

    /**
     * 在指定本地磁盘路径下，读取指定大小的内容后返回
     */
    @RequestMapping("read")
    public ResponseEntity<FileSystemResource> readFile(@RequestParam String path) throws IOException {
        FileSystemResource fileSystemResource = dataService.read(path);
        return ResponseEntity.ok()
                .contentType(MediaType.APPLICATION_OCTET_STREAM)
                .contentLength(fileSystemResource.contentLength())
                .body(fileSystemResource);
    }

    /**
     * 删除本地文件
     */
    @PostMapping("delete-local")
    public ApiResponse deleteFile(@RequestParam String path) {
        dataService.deleteFile(path);
        return ApiResponse.success();
    }

    /**
     * 检测本地文件是否存在
     */
    @PostMapping("check")
    public ApiResponse checkFile(@RequestBody List<StatInfo> statInfoList) {
        return ApiResponse.success(dataService.checkFile(statInfoList));
    }

    /**
     * 将本地文件恢复到其他ds
     */
    @PostMapping("recover")
    public ApiResponse recoverFile(@RequestBody List<ReplicaData> replicaDataList) {
        boolean isSuccess = dataService.writeToOtherDs(replicaDataList.get(0).getPath(), null, replicaDataList);
        return isSuccess ? ApiResponse.success() : ApiResponse.failure("恢复副本失败");
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }
}

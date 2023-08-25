package com.ksyun.campus.metaserver.controller;

import com.ksyun.campus.common.domain.FileType;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import com.ksyun.campus.common.dto.CommitWriteDTO;
import com.ksyun.campus.common.response.ApiResponse;
import com.ksyun.campus.metaserver.services.MetaService;
import org.springframework.web.bind.annotation.*;

import java.util.HashSet;
import java.util.List;

@RestController("/")
public class MetaController {

    private final MetaService metaService;

    public MetaController(MetaService metaService) {
        this.metaService = metaService;
    }

    @RequestMapping("stats")
    public ApiResponse stats(@RequestHeader String fileSystem, @RequestParam String path) {
        return ApiResponse.success(metaService.getFileStats(fileSystem, path));
    }

    @RequestMapping("create")
    public ApiResponse createFile(@RequestHeader String fileSystem, @RequestParam String path) {
        // 创建元数据
        metaService.createDirOrFile(fileSystem, path, FileType.File);
        // 获取三副本server
        List<ReplicaData> threeReplica = metaService.pickDataServer(3, new HashSet<>(), null);
        return ApiResponse.success(threeReplica);
    }

    @RequestMapping("mkdir")
    public ApiResponse mkdir(@RequestHeader String fileSystem, @RequestParam String path) {
        metaService.createDirOrFile(fileSystem, path, FileType.Directory);
        return ApiResponse.success();
    }

    @RequestMapping("listdir")
    public ApiResponse listdir(@RequestHeader String fileSystem, @RequestParam String path) {
        List<StatInfo> list = metaService.listDir(fileSystem, path);
        return ApiResponse.success(list);
    }

    @RequestMapping("delete")
    public ApiResponse delete(@RequestHeader String fileSystem, @RequestParam String path) {
        metaService.delete(fileSystem, path);
        return ApiResponse.success();
    }

    /**
     * 保存文件写入成功后的元数据信息，包括文件path、size、三副本信息等
     */
    @RequestMapping("commit-write")
    public ApiResponse commitWrite(@RequestHeader String fileSystem, @RequestBody CommitWriteDTO commitWriteDTO) {
        metaService.commitWrite(fileSystem, commitWriteDTO);
        return ApiResponse.success();
    }

    /**
     * 根据文件path查询三副本的位置，返回客户端具体ds、文件分块信息
     */
    @RequestMapping("open")
    public ApiResponse open(@RequestHeader String fileSystem, @RequestParam String path) {
        return ApiResponse.success(metaService.getFileStats(fileSystem, path));
    }

    /**
     * 关闭退出进程
     */
    @RequestMapping("shutdown")
    public void shutdownServer() {
        System.exit(-1);
    }

}

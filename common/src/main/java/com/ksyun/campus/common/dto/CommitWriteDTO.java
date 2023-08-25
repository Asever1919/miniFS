package com.ksyun.campus.common.dto;

import com.ksyun.campus.common.domain.ReplicaData;
import lombok.Data;

import java.util.List;

@Data
public class CommitWriteDTO {

    private String path;

    private Long fileSize;

    private List<ReplicaData> replicaDataList;
}

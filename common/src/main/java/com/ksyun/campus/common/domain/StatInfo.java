package com.ksyun.campus.common.domain;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;
import java.nio.file.Paths;
import java.util.List;

@Data
public class StatInfo implements Serializable
{
    private static final long serialVersionUID = 811348116944175951L;
    public String volume;
    public String path; // 不带volume
    public long size;
    public long mtime;
    public FileType type;
    public List<ReplicaData> replicaData;

    @JsonIgnore
    public String getZkPath() {
        return Paths.get("/fileSystem", volume, path).toString();
    }
}

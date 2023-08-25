package com.ksyun.campus.common.domain;

import lombok.Data;

import java.io.Serializable;

@Data
public class ReplicaData implements Serializable {
    private static final long serialVersionUID = -6760428606203279872L;
    private String id;
    private String dsNode;//格式为ip:port
    private String path;
}

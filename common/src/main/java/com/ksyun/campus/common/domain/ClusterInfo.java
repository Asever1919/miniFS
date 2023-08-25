package com.ksyun.campus.common.domain;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.util.List;

@Data
public class ClusterInfo {
    private MetaServerMsg masterMetaServer;
    private List<MetaServerMsg> slaveMetaServer;
    private List<DataServerMsg> dataServer;

    @Data
    public static class ServerMsg {
        private String host;
        private int port;
    }

    @EqualsAndHashCode(callSuper = true)
    @ToString(callSuper = true)
    @Data
    public static class MetaServerMsg extends ServerMsg {

    }

    @EqualsAndHashCode(callSuper = true)
    @ToString(callSuper = true)
    @Data
    public static class DataServerMsg extends ServerMsg {
        private int fileTotal;
        private long capacity;
        private long useCapacity; //单位字节
    }
}

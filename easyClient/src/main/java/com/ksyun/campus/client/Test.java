package com.ksyun.campus.client;

import com.ksyun.campus.client.util.ZkConnectUtil;
import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.util.ZkUtil;

import java.util.List;

public class Test {

    public static void main(String[] args) throws Exception {
        ZkUtil zkUtil = new ZkUtil(ZkConnectUtil.getClient());
        List<String> exists = zkUtil.getChildren("/servers/metaServer");
        System.out.println(exists);
        EFileSystem eFileSystem = new EFileSystem();
        ClusterInfo clusterInfo = eFileSystem.getClusterInfo();
        System.out.println(clusterInfo);
        Thread.sleep(10000);
    }
}

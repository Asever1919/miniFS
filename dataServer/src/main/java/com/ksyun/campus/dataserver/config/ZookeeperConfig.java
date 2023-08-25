package com.ksyun.campus.dataserver.config;

import com.ksyun.campus.common.domain.ClusterInfo;
import com.ksyun.campus.common.service.ZkServerService;
import com.ksyun.campus.common.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@Slf4j
public class ZookeeperConfig {

    private static final String connectString = System.getProperty("zookeeper.addr", "127.0.0.1:2181");

    private static final int sessionTimeout = 30000;

    // zk client 重试间隔时间
    private static final int baseSleepTimeMs = 5000;

    //zk client 重试次数
    private static final int retryCount = 10;

    private final String host = "127.0.0.1";

    @Value("${server.port}")
    private Integer port;

    private final String serviceName = "dataServer";

    @Bean
    CuratorFramework curatorFramework() {
        CuratorFramework client = CuratorFrameworkFactory
                .builder()
                .connectString(connectString)
                .sessionTimeoutMs(sessionTimeout)
                .retryPolicy(new ExponentialBackoffRetry(baseSleepTimeMs, retryCount))
                .build();
        client.start();
        return client;
    }

    @Bean
    ZkUtil zkUtil(CuratorFramework client) {
        return new ZkUtil(client);
    }

    @Bean
    ClusterInfo.DataServerMsg dataServerMsg() {
        ClusterInfo.DataServerMsg serverMsg = new ClusterInfo.DataServerMsg();
        serverMsg.setHost(host);
        serverMsg.setPort(port);
        serverMsg.setFileTotal(0);
        serverMsg.setCapacity(1024 * 1024 * 1024L); // 1GB
        serverMsg.setUseCapacity(0);
        return serverMsg;
    }

    @Bean
    ZkServerService zkServiceRegistry(CuratorFramework client, ClusterInfo.DataServerMsg serverMsg) {
        return new ZkServerService(client, serverMsg, serviceName);
    }
}
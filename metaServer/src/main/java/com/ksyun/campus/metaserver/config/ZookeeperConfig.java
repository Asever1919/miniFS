package com.ksyun.campus.metaserver.config;

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

    private static final int sessionTimeout = 2000;

    // zk client 重试间隔时间
    private static final int baseSleepTimeMs = 5000;

    //zk client 重试次数
    private static final int retryCount = 5;

    private String host = "127.0.0.1";

    @Value("${server.port}")
    private Integer port;

    private final String serviceName = "metaServer";

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
    ZkServerService zkServiceRegistry(CuratorFramework client) {
        ClusterInfo.MetaServerMsg metaServerMsg = new ClusterInfo.MetaServerMsg();
        metaServerMsg.setHost(host);
        metaServerMsg.setPort(port);
        return new ZkServerService(client, metaServerMsg, serviceName);
    }
}
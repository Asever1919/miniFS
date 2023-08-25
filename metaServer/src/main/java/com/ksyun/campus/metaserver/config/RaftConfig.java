package com.ksyun.campus.metaserver.config;

import com.alipay.sofa.jraft.conf.Configuration;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.NodeOptions;
import com.ksyun.campus.metaserver.raft.MetaServer;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer;
import org.springframework.boot.web.embedded.tomcat.TomcatServletWebServerFactory;
import org.springframework.boot.web.servlet.server.ConfigurableServletWebServerFactory;
import org.springframework.context.annotation.Bean;

import java.io.IOException;

@org.springframework.context.annotation.Configuration
@Slf4j
public class RaftConfig {

    @Value("${raft.dataPath}")
    private String dataPath;
    @Value("${raft.groupId}")
    private String groupId;
    @Value("${raft.serverIdStr}")
    private String serverIdStr;
    @Value("${raft.initConfStr}")
    private String initConfStr;

    @Data
    public static class ConfInfo {
        private String dataPath;
        private String groupId;
        private String serverIdStr;
        private String initConfStr;
    }

    @Bean
    public ConfInfo confInfo() {
        ConfInfo confInfo = new ConfInfo();
        confInfo.setDataPath(dataPath);
        confInfo.setGroupId(groupId);
        confInfo.setServerIdStr(serverIdStr);
        confInfo.setInitConfStr(initConfStr);
        return confInfo;
    }

    // 处理param中带特殊符号的问题
    @Bean
    public ConfigurableServletWebServerFactory webServerFactory() {
        TomcatServletWebServerFactory factory = new TomcatServletWebServerFactory();
        factory.addConnectorCustomizers((TomcatConnectorCustomizer) connector -> connector.setProperty("relaxedQueryChars", "|{}[]\\"));
        return factory;
    }

    @Bean
    MetaServer metaServer() throws IOException {
        final NodeOptions nodeOptions = new NodeOptions();
        // for test, modify some params
        // set election timeout to 1s
        nodeOptions.setElectionTimeoutMs(1000);
        // disable CLI service。
        nodeOptions.setDisableCli(false);
        // do snapshot every 30s
        nodeOptions.setSnapshotIntervalSecs(30);
        // parse server address
        final PeerId serverId = new PeerId();
        if (!serverId.parse(serverIdStr)) {
            throw new IllegalArgumentException("Fail to parse serverId:" + serverIdStr);
        }
        final Configuration initConf = new Configuration();
        if (!initConf.parse(initConfStr)) {
            throw new IllegalArgumentException("Fail to parse initConf:" + initConfStr);
        }
        // set cluster configuration
        nodeOptions.setInitialConf(initConf);
        // start raft server
        final MetaServer metaServer = new MetaServer(dataPath, groupId, serverId, nodeOptions);
        log.info("Started counter server at port:"
                + metaServer.getNode().getNodeId().getPeerId().getPort());
        return metaServer;
    }
}

package com.ksyun.campus.dataserver;

import com.ksyun.campus.common.service.ZkServerService;
import com.ksyun.campus.common.util.ZkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.Path;

@Component
@Slf4j
public class DataServerAppRunner implements ApplicationRunner {

    private final ZkUtil zkUtil;

    private final ZkServerService zkServerService;

    private final Path baseDataPath;

    public DataServerAppRunner(ZkUtil zkUtil, ZkServerService zkServerService, Path baseDataPath) {
        this.zkUtil = zkUtil;
        this.zkServerService = zkServerService;
        this.baseDataPath = baseDataPath;
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        zkServerService.register();
        zkServerService.updateDataServerInfo(false, new File(baseDataPath.toString()));
        zkUtil.addWatcherWithTreeCache("/servers", (client, event) -> {
            ChildData data = event.getData();
            if (data != null && event.getType() == TreeCacheEvent.Type.NODE_ADDED) {
                log.info("节点路径 - {}, 节点事件类型: {} , 节点值为: {}",
                        event.getData().getPath(), event.getType(), new String(data.getData()));
            }
        });
    }
}

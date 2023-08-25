package com.ksyun.campus.common.consts;

import java.nio.file.Path;
import java.nio.file.Paths;

public class ClientConstant {

    // 客户端临时目录
    public static final Path TMP_PATH = Paths.get(System.getProperty("user.dir"), "tmp");

}

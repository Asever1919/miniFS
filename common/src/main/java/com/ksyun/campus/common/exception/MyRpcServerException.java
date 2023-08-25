package com.ksyun.campus.common.exception;

public class MyRpcServerException extends RuntimeException {

    public MyRpcServerException() {
        super("Rpc服务端错误");
    }
}

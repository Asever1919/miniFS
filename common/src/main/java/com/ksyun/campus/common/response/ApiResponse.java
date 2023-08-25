package com.ksyun.campus.common.response;

import lombok.Data;

/**
 * 统一返回对象
 */
@Data
public class ApiResponse {

    private int code;

    private Object data;

    private String msg;

    public static ApiResponse success() {
        ApiResponse resp = new ApiResponse();
        resp.code = 200;
        return resp;
    }

    public static ApiResponse success(Object data) {
        ApiResponse resp = new ApiResponse();
        resp.code = 200;
        resp.data = data;
        return resp;
    }

    public static ApiResponse failure(String msg) {
        ApiResponse resp = new ApiResponse();
        resp.code = 500;
        resp.msg = msg;
        return resp;
    }

}

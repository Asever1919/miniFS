package com.ksyun.campus.metaserver.interceptor;

import org.apache.commons.lang.StringUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class ParamInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String path = request.getParameter("path");
        if (StringUtils.isNotEmpty(path) && !path.startsWith("/")) {
            throw new RuntimeException("path不是/开头");
        }
        return true;
    }
}

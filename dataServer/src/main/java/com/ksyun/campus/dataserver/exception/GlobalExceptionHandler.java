package com.ksyun.campus.dataserver.exception;

import com.ksyun.campus.common.response.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

@ControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    @ResponseBody
    public ApiResponse handleException(final Exception ex) {
        log.error("error", ex);
        return ApiResponse.failure(ex.getMessage());
    }
}

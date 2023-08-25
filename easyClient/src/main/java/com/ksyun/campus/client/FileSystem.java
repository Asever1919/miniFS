package com.ksyun.campus.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.client.util.HttpClientConfig;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.ZkConnectUtil;
import com.ksyun.campus.common.response.ApiResponse;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.URI;

public abstract class FileSystem {
    protected String fileSystem = "";
    private static HttpClient httpClient;
    protected final ObjectMapper mapper = new ObjectMapper();

    protected CloseableHttpResponse callRemote(HttpUriRequestBase request) throws IOException {
        try (CloseableHttpClient httpClient = (CloseableHttpClient) HttpClientUtil.createHttpClient(new HttpClientConfig());
             CloseableHttpResponse response = httpClient.execute(request)) {
            return response;
        }
    }

    protected ApiResponse callMetaServerByPath(String url, String path) throws Exception {
        String metaServerUrl = ZkConnectUtil.getMetaServerUrl() + url;
        URI uri = new URIBuilder(new URI(metaServerUrl))
                .addParameter("path", path)
                .build();
        HttpGet request = new HttpGet(uri);
        request.addHeader("fileSystem", fileSystem);
        try (CloseableHttpClient httpClient = (CloseableHttpClient) HttpClientUtil.createHttpClient(new HttpClientConfig());
             CloseableHttpResponse response = httpClient.execute(request)) {
            String entity = EntityUtils.toString(response.getEntity());
            return mapper.readValue(entity, ApiResponse.class);
        }
    }
}

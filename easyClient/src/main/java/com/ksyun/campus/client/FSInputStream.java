package com.ksyun.campus.client;

import com.ksyun.campus.client.util.HttpClientConfig;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.common.consts.ClientConstant;
import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.domain.StatInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.hc.client5.http.HttpHostConnectException;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.net.URIBuilder;

import java.io.*;
import java.net.URI;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class FSInputStream extends InputStream {

    private final FileInputStream fileInputStream;

    private final List<ReplicaData> replicaDataList;

    private final File tempFile;

    public FSInputStream(List<ReplicaData> replicaDataList, StatInfo statInfo) throws Exception {
        this.replicaDataList = replicaDataList;
        if (replicaDataList == null || replicaDataList.size() == 0) {
            log.error("dataServer列表为空");
            throw new RuntimeException("dataServer列表为空");
        }
        // 创建本地临时读取文件
        tempFile = new File(Paths.get(ClientConstant.TMP_PATH.toString(), "read_" + replicaDataList.get(0).getPath()).toUri());
        if (tempFile.exists() && statInfo.getSize() == tempFile.length()) {
            log.info("从本地获取缓存文件");
        } else {
            // 从ds获取
            tempFile.createNewFile();
            getFileFromDataServer();
        }
        this.fileInputStream = new FileInputStream(tempFile);
    }

    private void getFileFromDataServer() throws Exception {
        for (ReplicaData replicaData : replicaDataList) {
            String dataServerUrl = "http://" + replicaData.getDsNode() + ServerConstant.READ_URL;
            URI uri = new URIBuilder(new URI(dataServerUrl))
                    .addParameter("path", replicaData.getPath())
                    .build();
            HttpGet request = new HttpGet(uri);
            try (CloseableHttpClient httpClient = (CloseableHttpClient) HttpClientUtil.createHttpClient(new HttpClientConfig());
                 CloseableHttpResponse response = httpClient.execute(request)) {
                InputStream content = response.getEntity().getContent();
                FileUtils.copyToFile(content, tempFile);
                content.close();
                log.info("在: {} 成功获取到文件", replicaData.getDsNode());
                break;
            } catch (HttpHostConnectException e) {
                log.warn("连接到: {} 失败", replicaData.getDsNode());
            }
        }
        if (tempFile.length() == 0) {
            log.error("获取文件失败");
            throw new RuntimeException("获取文件失败");
        }
    }

    @Override
    public int read() throws IOException {
        return fileInputStream.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return super.read(b);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
        return super.read(b, off, len);
    }

    @Override
    public void close() throws IOException {
        super.close();
    }
}

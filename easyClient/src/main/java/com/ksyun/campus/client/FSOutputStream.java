package com.ksyun.campus.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ksyun.campus.client.util.HttpClientConfig;
import com.ksyun.campus.client.util.HttpClientUtil;
import com.ksyun.campus.client.util.ZkConnectUtil;
import com.ksyun.campus.common.consts.ClientConstant;
import com.ksyun.campus.common.consts.ServerConstant;
import com.ksyun.campus.common.domain.ReplicaData;
import com.ksyun.campus.common.dto.CommitWriteDTO;
import com.ksyun.campus.common.response.ApiResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.entity.mime.MultipartEntityBuilder;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class FSOutputStream extends OutputStream {

    private final FileOutputStream fileOutputStream;

    private final List<ReplicaData> replicaDataList;

    private final File tempFile;

    private final ObjectMapper mapper = new ObjectMapper();

    private final String path;

    private final String fileSystem;

    FSOutputStream(List<ReplicaData> replicaDataList, String path, String fileSystem) throws IOException {
        this.replicaDataList = replicaDataList;
        this.path = path;
        this.fileSystem = fileSystem;
        if (replicaDataList.size() == 0) {
            log.error("dataServer列表为空");
            throw new RuntimeException("dataServer列表为空");
        }
        // 创建本地临时写入文件
        tempFile = new File(Paths.get(ClientConstant.TMP_PATH.toString(), "write_" + replicaDataList.get(0).getPath()).toUri());
        tempFile.createNewFile();
        this.fileOutputStream = new FileOutputStream(tempFile);
    }

    @Override
    public void write(int b) throws IOException {
        fileOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        super.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        super.write(b, off, len);
    }

    @Override
    public void close() throws IOException {
        super.close();
        log.info("三副本信息:{}", mapper.writeValueAsString(replicaDataList));
        boolean isUploaded = uploadToDataServer();
        if (isUploaded) {
            commitWrite();
        }
    }

    /**
     * 提交到metaServer
     */
    private void commitWrite() {
        try {
            String metaServerUrl = ZkConnectUtil.getMetaServerUrl() + ServerConstant.COMMIT_WRITE_URL;
            CommitWriteDTO commitWriteDTO = new CommitWriteDTO();
            commitWriteDTO.setFileSize(tempFile.length());
            commitWriteDTO.setPath(path);
            commitWriteDTO.setReplicaDataList(replicaDataList);
            String json = mapper.writeValueAsString(commitWriteDTO);
            HttpEntity httpEntity = new StringEntity(json, ContentType.APPLICATION_JSON);
            HttpPost request = new HttpPost(metaServerUrl);
            request.setEntity(httpEntity);
            request.setHeader("fileSystem", fileSystem);
            try (CloseableHttpClient httpClient = (CloseableHttpClient) HttpClientUtil.createHttpClient(new HttpClientConfig());
                 CloseableHttpResponse response = httpClient.execute(request)) {
                String entity = EntityUtils.toString(response.getEntity());
                ApiResponse apiResponse = mapper.readValue(entity, ApiResponse.class);
                if (apiResponse.getCode() == 200) {
                    log.info("提交写成功");
                } else {
                    log.warn("提交写失败");
                }
            }
        } catch (Exception e) {
            log.error("提交写失败", e);
        }
    }

    /**
     * 上传到dataServer
     */
    private boolean uploadToDataServer() throws IOException {
        ReplicaData replicaData = replicaDataList.get(0);
        String dataServerUrl = "http://" + replicaData.getDsNode() + ServerConstant.WRITE_URL;
        HttpPost request = new HttpPost(dataServerUrl);
        HttpEntity httpEntity = MultipartEntityBuilder.create()
                .setCharset(StandardCharsets.UTF_8)
                .setContentType(ContentType.MULTIPART_FORM_DATA)
                .addBinaryBody("file", tempFile)
                .addTextBody("path", replicaData.getPath())
                .addTextBody("replicaDataListJson", mapper.writeValueAsString(replicaDataList))
                .build();
        request.setEntity(httpEntity);
        try (CloseableHttpClient httpClient = (CloseableHttpClient) HttpClientUtil.createHttpClient(new HttpClientConfig());
             CloseableHttpResponse response = httpClient.execute(request)) {
            String entity = EntityUtils.toString(response.getEntity());
            ApiResponse apiResponse = mapper.readValue(entity, ApiResponse.class);
            if (apiResponse.getCode() == 200) {
                log.info("上传dataServer:{} 成功", replicaData.getDsNode());
                return true;
            } else {
                log.warn("上传dataServer:{} 失败, {}", replicaData.getDsNode(), apiResponse.getMsg());
            }
        } catch (ParseException e) {
            log.error("http response解析错误", e);
        }
        return false;
    }
}

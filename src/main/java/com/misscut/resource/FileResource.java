package com.misscut.resource;

import com.futureinteraction.utils.FileProc;
import com.futureinteraction.utils.HttpUtils;
import com.futureinteraction.utils.RedisHolder;
import com.google.gson.Gson;
import com.misscut.event.EventConst;
import com.misscut.utils.FileMsPathProc;
import com.misscut.utils.ResponseUtils;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.file.FileProps;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author wangwentao
 * @date 2024/03/18 上午8:58
 */
@Slf4j
public class FileResource {

    private Gson gson = new Gson();

    private RedisHolder redis;
    private WebClient webClient;

    String PATH = "/file";

    public void register(Vertx vertx, Router mainRouter, Router router, RedisHolder redis) {
        this.redis = redis;
        this.webClient = WebClient.create(vertx);

        router.post("/upload").handler(this::uploadFile);
        router.get("/download").handler(this::downloadFile);

        mainRouter.mountSubRouter(PATH, router);
        HttpUtils.dumpRestApi(router, PATH, log);
    }


    private void uploadFile(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        AtomicLong time = new AtomicLong(new Date().getTime());
        String uuid = FileProc.genFileUuid(time.get());
        List<FileUpload> fileUploads = context.fileUploads();
        if (fileUploads.isEmpty()) {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            response.end(ResponseUtils.response(HttpStatus.SC_BAD_REQUEST, "empty file", null).encodePrettily());
            return;
        }

        Promise<Message<JsonObject>> filePromise = Promise.promise();
        Iterator<FileUpload> fileUploadIterator = fileUploads.iterator();
        List<FileUpload> files = new ArrayList<>();
        while (fileUploadIterator.hasNext()) {
            files.add(fileUploadIterator.next());
        }
        try {
            String fileName = URLDecoder.decode(files.get(0).fileName(), StandardCharsets.UTF_8);
            String filePath = files.get(0).uploadedFileName();
            Vertx vertx = context.vertx();

            Promise<Buffer> bufferPromise = Promise.promise();
            vertx.fileSystem().readFile(filePath, bufferPromise);
            bufferPromise.future().compose(v -> {
                JsonObject para = new JsonObject();
                Base64.Encoder encoder = Base64.getEncoder();
                String base64Str = encoder.encodeToString(v.getBytes());
                para.put(EventConst.FILES.REQ.KEYS.BASE64_DATA, base64Str)
                        .put(EventConst.FILES.REQ.KEYS.NAME, fileName)
                        .put(EventConst.FILES.REQ.KEYS.TIME, time)
                        .put(EventConst.FILES.REQ.KEYS.UUID, uuid);

//                log.debug("ready to upload fms: {}", para.encodePrettily());
                DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.FILES.REQ.ACTIONS.ADD_OR_UPDATE_ONE_BY_UUID);
                context.vertx().eventBus().request(EventConst.FILES.ID, para, d, filePromise);

                return filePromise.future();
            }).onSuccess(success -> {
                log.debug("upload file succeed");
                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "上传成功", uuid).encodePrettily());
            }).onFailure(failure -> {
                log.error("upload file failure: {}", failure.getMessage());
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, "上传失败", null).encodePrettily());
            }).onComplete(done -> vertx.fileSystem().delete(filePath)
                    .onFailure(failure -> log.error("delete tmp file fail: {}", filePath))
                    .onSuccess(success -> log.debug("delete tmp file succeed, {}", filePath)));
        } catch (Exception e) {
            log.error("upload ai_assistant portrait exception", e);
        }
    }


    private void downloadFile(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        String uuid = context.request().getParam("file_uuid");
        JsonObject filePara = new JsonObject().put("uuid", uuid);
        log.debug("file param:{}", filePara.encode());

        DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.FILES.REQ.ACTIONS.GET_FILE_BY_UUID);
        context.vertx().eventBus().<JsonObject>request(EventConst.FILES.ID, filePara, d, handler -> {
            if (handler.failed()) {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, handler.cause().getMessage(), null).encodePrettily());
            } else {
                String portraitName = handler.result().body().getString(EventConst.FILES.REPLY.COMMON_KEYS.FILE_NAME);
                String portraitPath = handler.result().body().getString(EventConst.FILES.REPLY.COMMON_KEYS.FILE_PATH);
                log.debug("portrait from fms, filePath:  {} ,fileName:  {}", portraitPath, portraitName);
                if (portraitPath != null)
                    FileResource.setFileHttpResponse(context.vertx(), response, portraitName, FileMsPathProc.getFilePath(portraitPath));
                else {
                    response.setStatusCode(HttpStatus.SC_NOT_FOUND);
                    response.end(ResponseUtils.response(HttpStatus.SC_NOT_FOUND, "no such portrait", null).encodePrettily());
                }
            }
        });
    }


    public static void setFileHttpResponse(Vertx vertx, HttpServerResponse response, String fileName, String filePath) {
        Promise<Boolean> promise = Promise.promise();
        vertx.fileSystem().exists(filePath, promise);
        promise.future().compose(v -> {
            Promise<FileProps> subPromise = Promise.promise();
            if (!v)
                subPromise.fail("no such file: file path, " + filePath);
            else
                vertx.fileSystem().lprops(filePath, subPromise);
            return subPromise.future();
        }).onComplete(handler -> {
            if (handler.failed()) {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, handler.cause().getMessage(), null).encodePrettily());
            } else {
                response.setStatusCode(HttpStatus.SC_OK);

                FileProps fileProps = handler.result();

                String contentType = null;
                if (fileName != null) {
                    String suffix = FileProc.getFileSuffix(fileName);
                    if (suffix != null) {
                        if (suffix.equalsIgnoreCase("apk"))
                            contentType = "application/apk";
                        else
                            contentType = "application/" + suffix;
                    }
                }

                if (contentType == null)
                    contentType = "application/octet-stream";

                response.putHeader(HttpHeaders.CONTENT_TYPE, contentType)
                        .putHeader(HttpHeaders.CONTENT_DISPOSITION, "attatchment; filename=\"" + fileName + "\"")
                        .putHeader(HttpHeaders.CONTENT_LENGTH, Long.toString(fileProps.size()))
                        .putHeader(HttpHeaders.CONTENT_ENCODING, HttpHeaders.IDENTITY)
                        .sendFile(filePath);
            }
        });
    }


}


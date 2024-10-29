package com.misscut.utils;

import com.misscut.config.SysConfigPara;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;


/**
 * @Author WangWenTao
 * @Date 2024-07-31 15:44:02
 **/

@Slf4j
public class WebUtils {

    public static Future<Buffer> doPush(WebClient webClient, JsonObject para, String action) {
        Promise<Buffer> promise = Promise.promise();
        HttpRequest<Buffer> post = webClient
                .post(SysConfigPara.conf.websocket_api.server_port, SysConfigPara.conf.websocket_api.server_ip, SysConfigPara.conf.websocket_api.push_url);
        post.putHeader("content-type", "application/json");
        post.addQueryParam("action", action);

        post.sendJsonObject(para).onSuccess(success -> {
            Buffer body = success.body();
            promise.complete(body);
        }).onFailure(failure -> {
            promise.fail(failure.getMessage());
        });

        return promise.future();

    }


    public static void queryUserStatus(WebClient webClient, int clientType, String userUid, Promise<String> promise) {
        HttpRequest<Buffer> req = webClient
                .get(SysConfigPara.conf.websocket_api.server_port, SysConfigPara.conf.websocket_api.server_ip, SysConfigPara.conf.websocket_api.user_status_url);
        req.putHeader("content-type", "application/json");
        req.addQueryParam("client_type", String.valueOf(clientType));
        req.addQueryParam("user_uid", userUid);

        req.send().onSuccess(success -> {
            JsonObject body = success.bodyAsJsonObject();
            String data = body.getBoolean("data") ? "online" : "offline";
            promise.complete(data);
        }).onFailure(failure -> {
            promise.fail(failure.getMessage());
        });
    }

}

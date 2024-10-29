package com.misscut.utils;

import com.misscut.config.SysConfigPara;
import com.misscut.model.ConstDef;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.util.Random;

/**
 * @Author WangWenTao
 * @Date 2024-08-05 17:49:28
 **/
@Slf4j
public class SmsUtils {

    public static void sendMessage(WebClient webClient, String phone, String checkNo, Promise<Void> promise) {

        HttpRequest<Buffer> request = webClient.post(SysConfigPara.conf.mal_api.server_port, SysConfigPara.conf.mal_api.server_ip, SysConfigPara.conf.mal_api.path).ssl(true);

        JsonObject data = new JsonObject();
        data.put("phones", new JsonArray().add(phone));
        data.put("sign_id", SysConfigPara.conf.sms_config.sign_id);
        data.put("template_id", SysConfigPara.conf.sms_config.template_id);
        data.put("para", new JsonArray().add(checkNo));

        JsonObject param = new JsonObject().put("text", data.encode()).put("type", SysConfigPara.conf.mal_api.sms_type);
        request.sendJson(param).onFailure(failure -> {
            promise.fail(failure.getMessage());
            log.error("failed to send msg {}: {}", phone, failure.getMessage());
        }).onSuccess(success -> {
            JsonObject rt = success.bodyAsJsonObject();
            Integer code = rt.getInteger("code");
            if (code != HttpStatus.SC_OK) {
                log.error("failed to send msg {}: {}-{}", phone, code, rt.encode());
                promise.fail("fail");
            } else {
                log.debug("succeeded to send msg {}: sum {}", phone, rt.encodePrettily());
                promise.complete();
            }
        });
    }

}

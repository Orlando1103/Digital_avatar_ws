package com.misscut.cache;

import com.futureinteraction.utils.RedisHolder;
import com.misscut.model.RedisKeyConstDef;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UserCheckNoCache {

    //600秒失效
    private static int MAX_TIME = 600;

    public static void init(int maxTime) {
        MAX_TIME = maxTime;
    }

    public static void setCheckNo(RedisHolder redis, String phone, String userIdentity, String checkNo, Promise<Void> promise) {
        String key = RedisKeyConstDef.USER.CHECK_NO + phone + ":" + userIdentity;
        List<String> values = new ArrayList<>();
        values.add(key);
        JsonObject data = new JsonObject();
        data.put("check-no", checkNo);

        values.add(data.encode());

        redis.doSet(values).onComplete(done -> {
            if (done.failed()) {
                promise.fail(done.cause());
                log.error(done.cause().getMessage());
            } else {
                promise.complete();
                log.info("set checkNo succeed: " + phone);

                redis.doExpire(key, MAX_TIME).onFailure(failure -> {
                    log.error(failure.getMessage());
                });
            }
        });
    }

    public static void getCheckNo(RedisHolder redis, String phone, String userIdentity, Promise<String> promise) {
        redis.doGet(RedisKeyConstDef.USER.CHECK_NO + phone + ":" + userIdentity).onComplete(done -> {
            if (done.failed()) {
                promise.fail(done.cause());
                log.error(done.cause().getMessage());
            } else {
                Response rt = done.result();
                if (rt != null) {
                    JsonObject jo = new JsonObject(rt.toString());
                    promise.complete(jo.getString("check-no"));
                } else
                    promise.complete();
            }
        });
    }

    public static void delCheckNo(RedisHolder redis, String phone, String userIdentity) {
        List<String> values = new ArrayList<>();
        values.add(RedisKeyConstDef.USER.CHECK_NO + phone + ":" + userIdentity);

        redis.doDel(values).onComplete(done -> {
            if (done.failed()) {
                log.error(done.cause().getMessage());
            } else {
                log.info("remove user check no {}; deleted {}", phone, done.result());
            }
        });
    }
}
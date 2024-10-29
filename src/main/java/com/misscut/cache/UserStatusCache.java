package com.misscut.cache;

import com.futureinteraction.utils.RedisHolder;
import com.misscut.model.RedisKeyConstDef;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * @Author WangWenTao
 * @Date 2024-08-08 16:17:42
 **/
@Slf4j
public class UserStatusCache {

    private static final int MAX_TIME = 24 * 3600;

    private static String getKey(String uid) {
        return RedisKeyConstDef.USER.PRESENCE_STATUS + uid;
    }

    public static Future<Response> setUserStatus(RedisHolder redis, String uid, String status) {
        Promise<Response> promise = Promise.promise();
        redis.doSet(List.of(getKey(uid), status))
                .onSuccess(success -> {
                    promise.complete(success);
                    redis.doExpire(getKey(uid), MAX_TIME).onFailure(failure -> {
                        log.error("set user status expire failure: {}, {}, {}", uid, status, failure.getMessage());
                    });
                })
                .onFailure(promise::fail);

        return promise.future();
    }


    public static Future<Response> getUserStatus(RedisHolder redis, String uid) {
        Promise<Response> promise = Promise.promise();
        redis.doGet(getKey(uid))
                .onSuccess(promise::complete)
                .onFailure(promise::fail);

        return promise.future();
    }



}

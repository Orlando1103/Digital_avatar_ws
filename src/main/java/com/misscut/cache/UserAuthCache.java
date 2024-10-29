package com.misscut.cache;

import com.futureinteraction.utils.DateFormatUtils;
import com.futureinteraction.utils.RedisHolder;
import com.misscut.model.ConstDef;
import com.misscut.model.RedisKeyConstDef;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.Response;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class UserAuthCache {
    // a week
    private static final int MAX_TIME = 24 * 3600 * 7;

    private static String getKey(String uid) {
        return RedisKeyConstDef.USER.AUTH + uid;
    }

    private static String getWebKey(String uid) {
        return RedisKeyConstDef.USER.AUTH + RedisKeyConstDef.LOGIN_TYPE.WEB + ":" + uid;
    }

    private static String getAndroidKey(String uid) {
        return RedisKeyConstDef.USER.AUTH + RedisKeyConstDef.LOGIN_TYPE.APP + ":" + uid;
    }


    public static void setAuthData(RedisHolder redis, String uid, String devSerial, String loginTypeFlag, Handler<AsyncResult<Void>> handler) {

        String redisUserKey;
        List<String> values = new ArrayList<>();
        if (loginTypeFlag.equals(RedisKeyConstDef.LOGIN_TYPE.WEB)) {
            redisUserKey = getWebKey(uid);
        } else {
            redisUserKey = getAndroidKey(uid);
        }

        values.add(redisUserKey);
        values.add(ConstDef.JWT.TIME);
        values.add(DateFormatUtils.formatTime(System.currentTimeMillis()));
        values.add(ConstDef.JWT.DEV_SERIAL);
        values.add(devSerial);

        redis.doHset(values).onComplete(done -> {
            if (done.failed()) {
                log.error(done.cause().getMessage());
                handler.handle(Future.failedFuture(done.cause()));
            } else {
                log.debug("add user auth: {}", values);
                handler.handle(Future.succeededFuture());

                redis.doExpire(redisUserKey, MAX_TIME).onFailure(failure -> {
                    log.error(failure.getMessage());
                });
            }
        });
    }

    public static void updateAuthTime(RedisHolder redis, String uid, Handler<AsyncResult<Void>> handler) {

        List<String> values = new ArrayList<>();
        String key = getKey(uid);
        values.add(key);
        values.add(ConstDef.JWT.TIME);
        values.add(DateFormatUtils.formatTime(System.currentTimeMillis()));

        redis.doHset(values).onComplete(done -> {
            if (done.failed()) {
                done.cause().printStackTrace();
                log.error(done.cause().getMessage());
                handler.handle(Future.failedFuture(done.cause()));
            } else {
                log.debug("update user auth: {}}", values);
                handler.handle(Future.succeededFuture());

                redis.doExpire(key, MAX_TIME).onFailure(failure -> {
                    failure.printStackTrace();
                    log.error(failure.getMessage());
                });
            }
        });
    }


    public static void getAuthData(RedisHolder redis, String uid, String loginTypeFlag, Handler<AsyncResult<JsonObject>> handler) {
        String redisUserKey;
        if (loginTypeFlag.equals(RedisKeyConstDef.LOGIN_TYPE.WEB)) {
            redisUserKey = getWebKey(uid);
        } else {
            redisUserKey = getAndroidKey(uid);
        }
        redis.doHgetAll(redisUserKey).onComplete(done -> {
            if (done.failed()) {
                log.error(done.cause().getMessage());
                handler.handle(Future.failedFuture(done.cause()));
            } else {
                Response rt = done.result();
                JsonObject jo = new JsonObject();
                if (rt.containsKey(ConstDef.JWT.TIME)) {
                    jo.put(ConstDef.JWT.TIME, rt.get(ConstDef.JWT.TIME).toString());
                }
                if (rt.containsKey(ConstDef.JWT.DEV_SERIAL)) {
                    jo.put(ConstDef.JWT.DEV_SERIAL, rt.get(ConstDef.JWT.DEV_SERIAL).toString());
                }
                handler.handle(Future.succeededFuture(jo));
            }
        });
    }


    public static void del(RedisHolder redis, String uid, Handler<AsyncResult<Long>> handler) {
        String key = getKey(uid);
        List<String> para = new ArrayList<>();
        para.add(key);

        redis.doHdel(para).onComplete(done -> {
            if (done.failed()) {
                log.error(done.cause().getMessage());
                handler.handle(Future.failedFuture(done.cause()));
            } else {
                Response rt = done.result();
                handler.handle(rt != null ? Future.succeededFuture(Long.parseLong(rt.toString())) : Future.succeededFuture(0L));
            }
        });
    }

    /**
     * 清空用户登录认证信息缓存
     *
     * @param redis
     * @param uid
     * @param handler
     */
    public static void delAuthData(RedisHolder redis, String uid, String loginTypeFlag, Handler<AsyncResult<Long>> handler) {

        String redisUserKey = "";
        if (loginTypeFlag.equals(RedisKeyConstDef.LOGIN_TYPE.WEB)) {
            redisUserKey = getWebKey(uid);
        } else {
            redisUserKey = getAndroidKey(uid);
        }

        List<String> para = new ArrayList<>();
        para.add(redisUserKey);

        redis.doDel(para).onComplete(done -> {
            if (done.failed()) {
                log.error(done.cause().getMessage());
                handler.handle(Future.failedFuture(done.cause()));
            } else {
                Response rt = done.result();
                handler.handle(rt != null ? Future.succeededFuture(Long.parseLong(rt.toString())) : Future.succeededFuture(0L));
            }
        });
    }
}
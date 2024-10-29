package com.misscut.verticle;

import com.futureinteraction.utils.RedisHolder;
import com.google.gson.Gson;
import com.misscut.config.SysConfigPara;
import com.misscut.utils.CommonUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Context;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ScheduleVertical extends AbstractVerticle {

    private RedisHolder redis;

    private long timerId;
    
    private Gson gson = new Gson();

    @Override
    public void init(Vertx vertx, Context context) {
        super.init(vertx, context);
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        vertx.fileSystem().readFile("config-file/random_question.json", handle -> {
            if (handle.succeeded()) {
                Buffer bf = handle.result();
                JsonArray prologue = new JsonArray(new String(bf.getBytes()));
                CommonUtils.RANDOM_QUESTION = prologue.stream().map(o -> (String) o).collect(Collectors.toList());
                log.info("load random_question to cache succeed...");
            }
        });

        //启动文件监听
//        CommonUtils.onChangeFile();
        Promise<RedisHolder> promise = getRedisHolder();
        promise.future().onSuccess(ar -> {
            log.debug("start to checkChat2GenScoreMsg job ~");
            vertx.setPeriodic(30 *1000, id -> {
                CommonUtils.checkChat2GenScoreMsg(vertx, ar);
            });
            startPromise.complete();
        }).onFailure(fail -> {
            startPromise.fail(fail.getMessage());
        });
    }

    public Promise<RedisHolder> getRedisHolder() {
        Promise<RedisHolder> promise = Promise.promise();

        RedisHolder redis = new RedisHolder();

        SysConfigPara.RedisPara redisPara = gson.fromJson(config().getString("redis"), SysConfigPara.RedisPara.class);
        RedisOptions redisOptions = new RedisOptions();

        for (String host : redisPara.hosts)
            redisOptions.addConnectionString(host);

        if (redisPara.type != null)
            redisOptions.setType(RedisClientType.valueOf(redisPara.type));

        redisOptions.setMaxPoolSize(redisPara.max_pool_size > 0 ? redisPara.max_pool_size : 8);
        redisOptions.setMaxPoolWaiting(redisPara.max_pool_waiting > 0 ? redisPara.max_pool_waiting : 32);

        redis.init(vertx, redisOptions, redisPara.max_per_connect_retries, redisPara.max_reconnect_waiting_period, ar -> {
                    if (ar.failed()) {
                        promise.fail(ar.cause());
                    } else {
                        this.redis = redis;
                        promise.complete(redis);
                    }
                }
        );
        return promise;
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {
        vertx.cancelTimer(timerId);
        stopPromise.complete();
    }
}

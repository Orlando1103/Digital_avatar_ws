package com.misscut.verticle;

import com.futureinteraction.utils.RedisHolder;
import com.google.gson.Gson;
import com.misscut.config.SysConfigPara;
import com.misscut.event.ErrorCodes;
import com.misscut.event.EventConst;
import com.misscut.proxy.*;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlConnectOptions;
import lombok.extern.slf4j.Slf4j;

/**
 * @author jiyifei
 * @date 2023/5/18 上午10:04
 */
@Slf4j
public class DbVerticle extends AbstractVerticle {
    private Pool mySQLClient;
    private Gson gson = new Gson();
    public WebClient webClient;

    private UserProxy userProxy;
    private ChatProxy chatProxy;
    private CommonProxy commonProxy;
    private MessageProxy messageProxy;
    private MessageConsumer<JsonObject> userConsumer;
    private MessageConsumer<JsonObject> chatConsumer;
    private MessageConsumer<JsonObject> commonConsumer;
    private MessageConsumer<JsonObject> messageConsumer;

    private Promise<Void> startVerticle() {
        Promise<Void> promise = Promise.promise();

        if (!config().containsKey("db")
                || !config().containsKey("redis"))
            promise.fail("missing para");
        else {
            JsonObject dbPara = new JsonObject(new JsonObject(config().getString("db")).getString("db_para"));

            SqlConnectOptions connectOptions = new SqlConnectOptions()
                    .setPort(dbPara.getInteger("port"))
                    .setHost(dbPara.getString("host"))
                    .setDatabase(dbPara.getString("db"))
                    .setUser(dbPara.getString("user"))
                    .setPassword(dbPara.getString("password"));

            PoolOptions poolOptions = new PoolOptions().setMaxSize(dbPara.getInteger("max_pool_size"));

            mySQLClient = Pool.pool(vertx, connectOptions, poolOptions);

            RedisHolder redis = new RedisHolder();

            String redisParaString = config().getString("redis");
            SysConfigPara.RedisPara redisPara = gson.fromJson(redisParaString, SysConfigPara.RedisPara.class);

            final RedisOptions redisOptions = new RedisOptions();

            int maxPoolSize = redisPara.max_pool_size > 0 ? redisPara.max_pool_size : 8;
            redisOptions.setMaxPoolSize(maxPoolSize);
            int maxPoolWaiting = redisPara.max_pool_waiting > 0 ? redisPara.max_pool_waiting : 32;
            redisOptions.setMaxPoolWaiting(maxPoolWaiting);

            redisOptions.setPoolCleanerInterval(1000);

            if (redisPara.type != null)
                redisOptions.setType(RedisClientType.valueOf(redisPara.type));

            for (String host : redisPara.hosts)
                redisOptions.addConnectionString(host);

            redis.init(
                    vertx,
                    redisOptions,
                    redisPara.max_per_connect_retries,
                    redisPara.max_reconnect_waiting_period,
                    handler -> {
                        if (handler.failed()) {
                            promise.fail(handler.cause());
                        } else {
                            userProxy = new UserProxy(mySQLClient, redis, webClient, vertx);
                            userConsumer = vertx.eventBus().consumer(EventConst.USER.ID, this::onUserProxy);

                            chatProxy = new ChatProxy(mySQLClient, redis, webClient, vertx);
                            chatConsumer = vertx.eventBus().consumer(EventConst.CHAT.ID, this::onChatProxy);

                            commonProxy = new CommonProxy(mySQLClient, redis, webClient, vertx);
                            commonConsumer = vertx.eventBus().consumer(EventConst.COMMON.ID, this::onCommonProxy);

                            messageProxy = new MessageProxy(mySQLClient, redis, webClient, vertx);
                            messageConsumer = vertx.eventBus().consumer(EventConst.MESSAGE.ID, this::onMessageProxy);
                            promise.complete();
                        }
                    });

        }
        return promise;
    }

    private void onUserProxy(Message<JsonObject> message) {
        if (!message.headers().contains(BasicEventProcProxy.ACTION)) {
            log.error("No action header specified for message with headers {} and body {}", message.headers(), message.body().encodePrettily());
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED, "No action header specified");
            return;
        }
        userProxy.proc(message);
    }


    private void onChatProxy(Message<JsonObject> message) {
        if (!message.headers().contains(BasicEventProcProxy.ACTION)) {
            log.error("No action header specified for message with headers {} and body {}", message.headers(), message.body().encodePrettily());
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED, "No action header specified");
            return;
        }
        chatProxy.proc(message);
    }


    private void onCommonProxy(Message<JsonObject> message) {
        if (!message.headers().contains(BasicEventProcProxy.ACTION)) {
            log.error("No action header specified for message with headers {} and body {}", message.headers(), message.body().encodePrettily());
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED, "No action header specified");
            return;
        }
        commonProxy.proc(message);
    }


    private void onMessageProxy(Message<JsonObject> message) {
        if (!message.headers().contains(BasicEventProcProxy.ACTION)) {
            log.error("No action header specified for message with headers {} and body {}", message.headers(), message.body().encodePrettily());
            message.fail(ErrorCodes.NO_ACTION_SPECIFIED, "No action header specified");
            return;
        }
        messageProxy.proc(message);
    }


    @Override
    public void start(Promise<Void> promise) {
        startVerticle().future().onComplete(promise);
    }

    @Override
    public void stop() {

        mySQLClient.close();
        userConsumer.unregister();

        log.info("stop db vertical");
    }
}

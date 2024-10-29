package com.misscut.verticle;

import com.misscut.event.ErrorCodes;
import com.misscut.event.EventConst;
import com.misscut.utils.TimeUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

@Slf4j
public class AlgoVerticle extends AbstractVerticle {
    private MessageConsumer<JsonObject> algoConsumer;
    private WebClient webClient;
    private String HOST;
    private int PORT;

    private Promise<Void> startVerticle() {
        Promise<Void> promise = Promise.promise();

        HOST = config().getString("host");
        PORT = config().getInteger("port");

        WebClientOptions wco = new WebClientOptions();
        wco.setMaxPoolSize(config().getInteger("max_concurrency", 25));
        wco.setConnectTimeout(10000);
        wco.setIdleTimeout(35000);
        wco.setIdleTimeoutUnit(TimeUnit.MILLISECONDS);

        webClient = WebClient.create(vertx, wco);

        algoConsumer = vertx.eventBus().consumer(EventConst.ALGO_API.ID, this::callAlgo);
        promise.complete();

        return promise;
    }

    private void callAlgo(Message<JsonObject> msg) {
        JsonObject para = msg.body().getJsonObject(EventConst.ALGO_API.KEYS.PARA);
        String url = msg.body().getString(EventConst.ALGO_API.KEYS.URL);
        Boolean stream = msg.body().getBoolean(EventConst.ALGO_API.KEYS.STREAM, false);

        Promise<Object> promise = Promise.promise();
        getAlgoResponse(webClient, para, url, stream, promise);
        promise.future()
                .onFailure(failure -> msg.fail(ErrorCodes.ALGO_ERROR, failure.getMessage()))
                .onSuccess(msg::reply);
    }

    @Override
    public void start(Promise<Void> promise) {
        startVerticle().future().onComplete(promise);
    }

    @Override
    public void stop() {
        algoConsumer.unregister();
        if (webClient != null)
            webClient.close();

        log.info("stop algo verticle");
    }


    private void getAlgoResponse(WebClient webClient, JsonObject para, String url, Boolean stream, Promise<Object> promise) {
        long start = System.currentTimeMillis();
        HttpRequest<Buffer> post = webClient
                .post(PORT, HOST, url)
                .putHeader("content-type", "application/json");

        post.sendJsonObject(para).timeout(30, TimeUnit.SECONDS).onComplete(ar -> {
            long duration = System.currentTimeMillis() - start;
            if (ar.succeeded()) {
                Buffer body = ar.result().body();
                int code = ar.result().statusCode();

                log.debug("the mal response: {}, {}, cost {}ms", url, stream ? "stream" : body.toJsonObject().encodePrettily(), duration);

                if (code == HttpStatus.SC_OK) {
                    if (stream) {
                        promise.complete(body);
                    } else {
                        JsonObject result = body.toJsonObject().getJsonObject("result");
                        promise.complete(result);
                    }
                } else {
                    promise.fail("AI service returned error: " + code);
                }
            } else {
                log.error("the mal error: {}, {}", url, ar.cause().getMessage());
                promise.fail(ar.cause().getMessage());
            }
        });
    }

}

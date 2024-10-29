package com.misscut.verticle;

import com.futureinteraction.utils.FileProc;
import com.futureinteraction.utils.RedisHolder;
import com.google.gson.Gson;
import com.misscut.auth.JWTAuthProc;
import com.misscut.config.SysConfigPara;
import com.misscut.resource.*;
import com.misscut.utils.RequestLogHandler;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerFormat;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.redis.client.RedisClientType;
import io.vertx.redis.client.RedisOptions;
import org.apache.http.HttpHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class RestVerticle extends AbstractVerticle {

    private static Logger logger = LoggerFactory.getLogger(RestVerticle.class.getName());
    private HttpServer server;
    private Gson gson = new Gson();

    /**
     * This method constructs the router factory, mounts services and handlers and starts the http server with built router
     *
     * @return
     */
    private Future<Void> startHttpServer() {
        // Generate the router
        Router router = Router.router(vertx);

        router.route().handler(BodyHandler.create().setUploadsDirectory(FileProc.getDIR()).setBodyLimit(-1));
        router.route().handler(StaticHandler.create());
        router.route().handler(RequestLogHandler.create(LoggerFormat.CUSTOM));

        SysConfigPara.RedisPara redisPara = gson.fromJson(config().getString("redis"), SysConfigPara.RedisPara.class);
        RedisOptions redisOptions = new RedisOptions();
        redisOptions.addConnectionString(redisPara.hosts[0]);
        if (redisPara.type != null)
            redisOptions.setType(RedisClientType.valueOf(redisPara.type));

        redisOptions.setMaxPoolSize(redisPara.max_pool_size > 0 ? redisPara.max_pool_size : 8);
        redisOptions.setMaxPoolWaiting(redisPara.max_pool_waiting > 0 ? redisPara.max_pool_waiting : 32);
        redisOptions.setPoolCleanerInterval(1000);
        Promise<Void> redisPromise = Promise.promise();

        RedisHolder redis = new RedisHolder();
        redis.init(vertx, redisOptions, redisPara.max_per_connect_retries, redisPara.max_reconnect_waiting_period, redisPromise);

        JsonObject config = config();
        int port = config.getInteger("port");
        HttpServerOptions serverOptions = new HttpServerOptions();
        serverOptions
                .setPort(port)
                .setIdleTimeoutUnit(TimeUnit.SECONDS)
                .setIdleTimeout(300)
                .setCompressionSupported(true);

        logger.info("start http server at port: {}", port);

        server = vertx.createHttpServer(serverOptions).requestHandler(router);
        Promise<HttpServer> restPromise = Promise.promise();
        server.listen(restPromise);

        Promise<Void> promise = Promise.promise();
        CompositeFuture.all(redisPromise.future(), restPromise.future()).onSuccess(success -> {
            JWTAuthProc jwtAuthProc = new JWTAuthProc(redis);
            router.route().handler(jwtAuthProc);

            regRestApi(router);
            reg(vertx, router, redis);
            promise.complete();
        }).onFailure(promise::fail);

        return promise.future();
    }

    public static void reg(Vertx vertx, Router mainRouter, RedisHolder redis) {

        mainRouter.errorHandler(500, rc -> {
            Throwable failure = rc.failure();
            JsonObject resp = new JsonObject();
            String message = failure.getMessage();
            logger.error("",failure);
            resp.put("msg", message);
            rc.response()
                    .setStatusCode(500)
                    .end(resp.toString());
        });

        Router webResourceRouter = Router.router(vertx);
        new WebResource().register(vertx, mainRouter, webResourceRouter, redis);

        Router userResourceRouter = Router.router(vertx);
        new UserResource().register(vertx, mainRouter, userResourceRouter, redis);

        Router signinResourceRouter = Router.router(vertx);
        new SigninResource().register(vertx, mainRouter, signinResourceRouter, redis);

        Router signoutResourceRouter = Router.router(vertx);
        new SignoutResource().register(vertx, mainRouter, signoutResourceRouter, redis);

        Router fileResourceRouter = Router.router(vertx);
        new FileResource().register(vertx, mainRouter, fileResourceRouter, redis);
    }

    private void enableCorsSupport(Router router) {
        CorsHandler corsHandler = CorsHandler.create(".*");
        corsHandler.allowedMethod(HttpMethod.GET);
        corsHandler.allowedMethod(HttpMethod.POST);
        corsHandler.allowedMethod(HttpMethod.PUT);
        corsHandler.allowedMethod(HttpMethod.DELETE);
        corsHandler.allowedMethod(HttpMethod.OPTIONS);
        corsHandler.allowedHeader(HttpHeaders.AUTHORIZATION);
        corsHandler.allowedHeader(HttpHeaders.CONTENT_TYPE);
        corsHandler.allowedHeader("Origin");
        corsHandler.allowedHeader("Access-Control-Allow-Origin");
        corsHandler.allowedHeader("Access-Control-Allow-Headers");
        corsHandler.allowedHeader("Access-Control-Allow-Method");
        corsHandler.allowedHeader("Access-Control-Allow-Credentials");
        corsHandler.allowedHeader("Access-Control-Expose-Headers");;

        corsHandler.allowCredentials(true);

        router.route().handler(corsHandler);
    }

    private void regRestApi(Router mainRouter) {
        enableCorsSupport(mainRouter);

    }

    @Override
    public void start(Promise<Void> promise) {
        startHttpServer().onComplete(promise);
    }

    /**
     * This method closes the http server and unregister all services loaded to Event Bus
     */
    @Override
    public void stop() {
        server.close();
        System.out.println("stop rest server verticle");
    }
}
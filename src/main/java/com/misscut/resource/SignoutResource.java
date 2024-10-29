package com.misscut.resource;

import com.futureinteraction.utils.HttpUtils;
import com.futureinteraction.utils.RedisHolder;
import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import com.misscut.cache.UserAuthCache;
import com.misscut.model.ConstDef;
import com.misscut.utils.ResponseUtils;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;


@Slf4j
public class SignoutResource {

    private static String PATH = "/signout";

    private RedisHolder redisHolder;

    private Gson gson = new Gson();

    public void register(Vertx vertx, Router mainRouter, Router router, RedisHolder redis) {
        this.redisHolder = redis;

        router.get("/").handler(this::signOut);
        mainRouter.mountSubRouter(PATH, router);
        HttpUtils.dumpRestApi(router, PATH, log);
    }


    private void signOut(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);
        JsonObject userInfo = context.user().principal();
        String uid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        String loginTypeFlag = userInfo.getString(ConstDef.JWT.LOGIN_TYPE_FLAG);
        UserAuthCache.delAuthData(redisHolder, uid, loginTypeFlag, handler -> {
            if (handler.succeeded()) {
                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
            } else {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, handler.cause().getMessage(), null).encodePrettily());
            }
        });
    }
}

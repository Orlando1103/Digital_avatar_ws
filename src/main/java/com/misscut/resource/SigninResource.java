package com.misscut.resource;

import com.futureinteraction.utils.GetParaParser;
import com.futureinteraction.utils.HttpUtils;
import com.futureinteraction.utils.RedisHolder;
import com.futureinteraction.utils.StringUtils;
import com.google.gson.Gson;
import com.misscut.auth.JWTAuthProc;
import com.misscut.cache.UserAuthCache;
import com.misscut.cache.UserCheckNoCache;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.utils.CommonUtils;
import com.misscut.utils.ResponseUtils;
import com.misscut.utils.TimeUtils;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;

import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * @author wangwentao
 * @date 2024/03/18 上午8:58
 */
@Slf4j
public class SigninResource {

    private Gson gson = new Gson();

    private RedisHolder redis;
    private WebClient webClient;

    String PATH = "/signin";

    public void register(Vertx vertx, Router mainRouter, Router router, RedisHolder redis) {
        this.redis = redis;
        this.webClient = WebClient.create(vertx);

        router.get("/check_no").handler(this::signInByCheckNo);
        mainRouter.mountSubRouter(PATH, router);
        HttpUtils.dumpRestApi(router, PATH, log);
    }


    private void signInByCheckNo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        String phone = context.request().getParam("phone");
        String checkNo = context.request().getParam("check_no");
        String identity = context.request().getParam("identity");
        String loginTypeFlag = context.request().getParam("login-type-flag");

        String devSerial = UUID.randomUUID().toString();

        Promise<String> p1 = Promise.promise();
        UserCheckNoCache.getCheckNo(redis, phone, identity, p1);
        p1.future().onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            response.end(ResponseUtils.response(HttpStatus.SC_BAD_REQUEST, "验证码校验失败", null).encodePrettily());
        }).onSuccess(success -> {
            if (StringUtils.isNullOrEmpty(success) || !success.equals(checkNo)) {
                response.setStatusCode(HttpStatus.SC_NOT_ACCEPTABLE);
                response.end(ResponseUtils.response(HttpStatus.SC_NOT_ACCEPTABLE, "验证码无效或手机号码不匹配", null).encodePrettily());
            } else {
                UserCheckNoCache.delCheckNo(redis, phone, identity);

                DeliveryOptions d1 = new DeliveryOptions();
                d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_PAGE);
                GetParaParser.ListQueryPara p = new GetParaParser.ListQueryPara();
                p.queryParaList = List.of("[" + ConstDef.USER_BASIS_DATA_KEYS.USER_PHONE + ":=:" + phone + "]" +
                        "[" + ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY + ":=:" + identity + "]");
                JsonObject para = new JsonObject().put(EventConst.USER.KEYS.QUERY_PARA, gson.toJson(p));

                context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para, d1).onFailure(failure -> {
                    response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                    response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
                }).onSuccess(v -> {
                    boolean isFirstSign;
                    JsonObject body = v.body();
                    Integer num = body.getJsonArray("data").getInteger(0);
                    Promise<JsonObject> userPromise = Promise.promise();
                    if (num == 0) {
                        isFirstSign = true;
                        DeliveryOptions d2 = new DeliveryOptions();
                        d2.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.ADD);
                        JsonObject addPara = new JsonObject()
                                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, UUID.randomUUID().toString().replaceAll("-", ""))
                                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_PHONE, phone)
                                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY, identity)
                                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME, CommonUtils.getRandomBoyName())
                                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_REG_TIME, TimeUtils.getTime());

                        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, addPara, d2)
                                .onSuccess(v1 -> userPromise.complete(v1.body().getJsonObject("user_basis")))
                                .onFailure(failure -> userPromise.fail(failure.getMessage()));
                    } else {
                        JsonObject user = body.getJsonArray("data").getJsonArray(1).getJsonObject(0);
                        isFirstSign = false;
                        userPromise.complete(user);
                    }
                    userPromise.future().onSuccess(data -> {
                        log.trace("the data is: {}", data.encodePrettily());
                        data.put(ConstDef.JWT.DEV_SERIAL, devSerial);
                        data.put(ConstDef.JWT.TIME, new Date().getTime());
                        data.put(ConstDef.JWT.LOGIN_TYPE_FLAG, loginTypeFlag);
                        String token = JWTAuthProc.generateToken(data);
                        String uid = data.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
                        Promise<Void> authDataPromise = Promise.promise();
                        UserAuthCache.setAuthData(redis, uid, devSerial, loginTypeFlag, authDataPromise);
                        authDataPromise.future().onSuccess(succ -> {
                            JsonObject rt = new JsonObject()
                                    .put("token", token)
                                    .put("is_first_sign", isFirstSign)
                                    .put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, uid);
                            response.setStatusCode(HttpStatus.SC_OK);
                            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
                        }).onFailure(failure -> {
                            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
                        });
                    }).onFailure(failure -> {
                        response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                        response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
                    });
                });
            }
        });
    }
}


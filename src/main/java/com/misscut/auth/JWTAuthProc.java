package com.misscut.auth;

import com.futureinteraction.utils.RedisHolder;
import com.futureinteraction.utils.StringUtils;
import com.google.gson.Gson;
import com.misscut.cache.UserAuthCache;
import com.misscut.model.ConstDef;
import com.misscut.utils.ResponseUtils;
import io.vertx.core.*;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.User;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.RoutingContext;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;

import java.text.DateFormat;
import java.text.SimpleDateFormat;

@Slf4j
public class JWTAuthProc implements Handler<RoutingContext> {

    public static final String HEADER = "Bearer";
    public static final String JWT_AUTH_KEY = "token";
    @Getter
    private static JWTAuth jwtAuth;

    private Gson gson = new Gson();
    private RedisHolder redisHolder;

    private static String jwt_algorithm;
    private static String jwt_buffer;
    private static long MAX_TOKEN_TIME;

    /**
     * 使用threadLocal避免多线程
     */
    private static ThreadLocal<DateFormat> simpleDateFormat;

    public JWTAuthProc(RedisHolder redisHolder) {
        this.redisHolder = redisHolder;
    }

    public static void setJwtPara(String jwt_algorithm, String jwt_pulic_key, long max_token_time) {
        JWTAuthProc.jwt_algorithm = jwt_algorithm;
        JWTAuthProc.jwt_buffer = jwt_pulic_key;
        JWTAuthProc.MAX_TOKEN_TIME = max_token_time;
    }

    public static void init(Vertx vertx) {
        JWTAuthOptions jwtAuthOptions = new JWTAuthOptions()
                .addPubSecKey(new PubSecKeyOptions()
                        .setAlgorithm(jwt_algorithm)
                        .setBuffer(jwt_buffer));

        jwtAuth = JWTAuth.create(vertx, jwtAuthOptions);
        simpleDateFormat = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
    }

    public static String generateToken(JsonObject jsonObject) {
        return jwtAuth.generateToken(jsonObject);
    }
    @Override
    public void handle(RoutingContext context) {

        String uri = context.request().uri().toLowerCase();
        HttpMethod method = context.request().method();
        
        if (uri.startsWith("/signin")
                || uri.startsWith("/user/check_no")
                || uri.startsWith("/user/presence/notify")
//                || uri.startsWith("/web/ws/create_message")
                || uri.startsWith("/web/ws/handle_ai_assistant")
//                || uri.startsWith("/web/ws/get_message")
                || uri.startsWith("/web/ws/update_message")
                || uri.startsWith("/web/mal/save_logic")
                || uri.startsWith("/web/mal/get_logic")
        )
        {
            context.next();
        }
        else {
            String authorization = context.request().getHeader(HttpHeaders.AUTHORIZATION);
            String endpoint = context.request().getHeader("endpoint");
            Promise<User> authenticatePromise = Promise.promise();
            executeCommonUserAuthenticate(uri, endpoint, authorization, authenticatePromise);
            authenticatePromise.future().onSuccess(authenticateSuccess -> {
                context.setUser(authenticateSuccess);
                context.next();

            }).onFailure(fail -> {
                context.response().setStatusCode(HttpStatus.SC_UNAUTHORIZED)
                        .putHeader(io.vertx.core.http.HttpHeaders.CONTENT_TYPE, "application/json; charset=utf-8")
                        .end(ResponseUtils.response(HttpStatus.SC_UNAUTHORIZED, fail.getMessage(), null).encodePrettily());
            });
        }
    }


    /**
     * 普通用户认证
     * @param token
     * @param done
     */
    private void executeCommonUserAuthenticate(String uri, String endpoint, String token, Handler<AsyncResult<User>> done) {
        if (StringUtils.isNullOrEmpty(token) || !token.contains(HEADER)) {
            log.info("invalid token");
            done.handle(Future.failedFuture("请登录"));
            return;
        }
        //使用jwt进行认证
        String jwt = token.substring(Math.min(HEADER.length() + 1, token.length()));
        Promise<User> parseTokenPromise = Promise.promise();
        jwtAuth.authenticate(new JsonObject().put(JWT_AUTH_KEY, jwt), parseTokenPromise);
        parseTokenPromise.future().onSuccess(parseSuccess -> {
            User user = parseSuccess;

            JsonObject userInfo = user.principal();
            Integer userId = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
            if (userId == null) {
                done.handle(Future.failedFuture("登录已过期,请重新登录"));
                return;
            }
            log.trace("the userInfo and token: {}, {}", userInfo, token);

            long time = userInfo.getLong(ConstDef.JWT.TIME);
            long curTime = System.currentTimeMillis();
            long timeDiff = curTime - time;

            // check token time, excluding visitor
            if (userId != 0 && timeDiff > MAX_TOKEN_TIME) {
                done.handle(Future.failedFuture("登录已过期,请重新登录"));
                return;
            }
            String uid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
            String loginTypeFlag = userInfo.getString(ConstDef.JWT.LOGIN_TYPE_FLAG);

            // 获取用户缓存数据
            Promise<JsonObject> getAuthDataPromise = Promise.promise();
            if (uid != null) {
                UserAuthCache.getAuthData(redisHolder, uid, loginTypeFlag, getAuthDataPromise);
            } else {
                getAuthDataPromise.complete();
            }

            getAuthDataPromise.future().onSuccess(userAuthInCache -> {
                String devSerial = user.principal().getString(ConstDef.JWT.DEV_SERIAL);

                Promise<User> subPromise = Promise.promise();
                if (userAuthInCache == null || !devSerial.equalsIgnoreCase(userAuthInCache.getString(ConstDef.JWT.DEV_SERIAL))) {
                    log.trace("auth fail， uri: {}, user info: {}, endpoint -- {}", uri, userInfo, endpoint);
                    done.handle(Future.failedFuture("系统检测到登陆信息发生变化，需重新登陆验证"));
                } else {
                    done.handle(Future.succeededFuture(user));
                }
            }).onFailure(fail -> {
                log.error("get user auth data fail -- {}, user info: {}", fail.getMessage(), userInfo);
                done.handle(Future.failedFuture("获取信息失败，请重试！"));
            });
        }).onFailure(fail -> {
            log.error("jwt authenticate fail --- {}, jwt: {}, token: {}, uri:{}, endpoint: {}", fail.getMessage(), jwt, token, uri, endpoint);
            done.handle(Future.failedFuture("登录已过期,请重新登录"));
        });
    }

}
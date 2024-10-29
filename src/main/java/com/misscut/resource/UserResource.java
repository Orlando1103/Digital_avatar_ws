package com.misscut.resource;

import com.futureinteraction.utils.HttpUtils;
import com.futureinteraction.utils.RedisHolder;
import com.futureinteraction.utils.StringUtils;
import com.futureinteraction.utils.exception.InvalidParaException;
import com.google.gson.Gson;
import com.misscut.cache.UserCheckNoCache;
import com.misscut.cache.UserStatusCache;
import com.misscut.config.SysConfigPara;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.utils.CommonUtils;
import com.misscut.utils.ResponseUtils;
import com.misscut.utils.SmsUtils;
import com.misscut.utils.TimeUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wangwentao
 * @date 2024/03/18 上午8:58
 */
@Slf4j
public class UserResource {

    private Gson gson = new Gson();

    private RedisHolder redis;
    private WebClient webClient;

    String PATH = "/user";

    public void register(Vertx vertx, Router mainRouter, Router router, RedisHolder redis) {
        this.redis = redis;
        this.webClient = WebClient.create(vertx);

        router.get("/check_no").handler(this::getCheckNo);
        router.post("/save_parent_info").handler(this::saveParentInfo);
        router.post("/save_expert_info").handler(this::saveExpertInfo);
        router.post("/save_child_info").handler(this::saveChildInfo);
        router.post("/delete_child_info").handler(this::delChildInfo);
        router.post("/save_family_info").handler(this::saveFamilyInfo);

        router.get("/get_expert_info").handler(this::getExpertInfo);
        router.get("/get_parent_info").handler(this::getParentInfo);
        router.get("/get_chat_records").handler(this::getChatRecords);

        //来自websocket的用户在线状态的通知
        router.post("/presence/notify").handler(this::userPresenceNotify);

        router.post("/parent_join_org").handler(this::parentJoinOrg);
        router.get("/get_joined_org").handler(this::getJoinedOrg);
        router.post("/delete_parent_org").handler(this::delParentOrg);

        mainRouter.mountSubRouter(PATH, router);
        HttpUtils.dumpRestApi(router, PATH, log);
    }

    private void getChatRecords(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        if (userIdentity == ConstDef.USER_IDENTITY.EXPERT) {
            String parentUid = context.request().getParam("parent_id");
            JsonObject para = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID, parentUid).put(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID, userUid).put("isFilterByDeleted", false);
            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL_BY_PARA);
            context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, para, d).compose(v1 -> {
                JsonArray chats = v1.body();
                List<Future<Message<JsonObject>>> chatReplyFutures = new ArrayList<>();
                Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
                for (int i = 0; i < chats.size(); i++) {
                    String chatUuid = chats.getJsonObject(i).getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                    DeliveryOptions d5 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
                    JsonObject para5 = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, chatUuid);
                    Future<Message<JsonObject>> chatReplyBasisFut = context.vertx().eventBus().request(EventConst.COMMON.ID, para5, d5);
                    chatReplyFutures.add(chatReplyBasisFut);
                }
                Future.join(chatReplyFutures).onComplete(done -> promise.complete(Pair.of(chats, chatReplyFutures)));

                return promise.future();
            }).onSuccess(success -> {
                JsonArray chats = success.getLeft();
                List<Future<Message<JsonObject>>> right = success.getRight();
                for (int i = 0; i < right.size(); i++) {
                    Future<Message<JsonObject>> future = right.get(i);
                    if (future.succeeded()) {
                        JsonObject body = future.result().body();
                        chats.getJsonObject(i).put("event_summary", body.getString(ConstDef.REPLY_BASIS_DATA_KEYS.EVENT_SUMMARY));
                    } else {
                        chats.getJsonObject(i).put("event_summary", null);
                    }
                }
                JsonArray records = new JsonArray();
                for (int i = 0; i < chats.size(); i++) {
                    JsonObject jo = chats.getJsonObject(i);
                    String eventSummary = jo.getString("event_summary");
                    if (StringUtils.isNullOrEmpty(eventSummary))
                        continue;
                    JsonObject record = new JsonObject().put("record_time", TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME)))
                            .put("record_title", "第" + (i + 1) + "次咨询")
                            .put("record_content", eventSummary);
                    records.add(record);
                }
                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", records).encodePrettily());
            }).onFailure(failure -> {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
            });
        } else {
            JsonObject para = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID, userUid).put("isFilterByDeleted", false);
            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL_BY_PARA);
            List<Future<Message<JsonObject>>> expertBasisFutureList = new ArrayList<>();
            List<Future<Message<JsonObject>>> expertInfoFutureList = new ArrayList<>();
            List<Future<Message<JsonObject>>> chatReplyFutureList = new ArrayList<>();
            Map<String, JsonObject> userBasisMap = new HashMap<>();
            Map<Integer, JsonObject> userInfoMap = new HashMap<>();

            context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, para, d).compose(v1 -> {
                JsonArray chats = v1.body();
                List<JsonObject> list = chats.stream().map(x -> (JsonObject) x)
                        .filter(c -> !"system".equals(c.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID)))
                        .collect(Collectors.toList());

                Set<String> expertIds = new HashSet<>();
                for (JsonObject jo : list) {
                    expertIds.add(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID));
                }
                for (String uid : expertIds) {
                    JsonObject reqBody = new JsonObject();
                    reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, uid);
                    DeliveryOptions d1 = new DeliveryOptions();
                    d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                    Future<Message<JsonObject>> expertBasisFuture = context.vertx().eventBus().request(EventConst.USER.ID, reqBody, d1);
                    expertBasisFutureList.add(expertBasisFuture);
                }
                Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
                Future.join(expertBasisFutureList).onComplete(done -> promise.complete(Pair.of(new JsonArray(list), expertBasisFutureList)));

                return promise.future();
            }).compose(v1 -> {
                JsonArray chats = v1.getLeft();
                Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();

                for (Future<Message<JsonObject>> future : expertBasisFutureList) {
                    if (future.succeeded()) {
                        JsonObject basis = future.result().body();
                        userBasisMap.put(basis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID), basis);
                        Integer userId = basis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                        DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
                        JsonObject para2 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                        Future<Message<JsonObject>> expertInfoFuture = context.vertx().eventBus().request(EventConst.USER.ID, para2, d2);
                        expertInfoFutureList.add(expertInfoFuture);
                    }
                }
                Future.join(expertInfoFutureList).onComplete(done -> promise.complete(Pair.of(chats, expertInfoFutureList)));

                return promise.future();
            }).compose(success -> {
                JsonArray chats = success.getLeft();
                for (Future<Message<JsonObject>> future : expertInfoFutureList) {
                    if (future.succeeded()) {
                        JsonObject info = future.result().body();
                        userInfoMap.put(info.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID), info);
                    }
                }
                Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
                for (int i = 0; i < chats.size(); i++) {
                    String chatUuid = chats.getJsonObject(i).getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                    DeliveryOptions d5 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
                    JsonObject para5 = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, chatUuid);
                    Future<Message<JsonObject>> chatReplyBasisFut = context.vertx().eventBus().request(EventConst.COMMON.ID, para5, d5);
                    chatReplyFutureList.add(chatReplyBasisFut);
                }
                Future.join(chatReplyFutureList).onComplete(done -> promise.complete(Pair.of(chats, chatReplyFutureList)));

                return promise.future();
            }).onSuccess(success -> {
                List<Future<Message<JsonObject>>> right = success.getRight();
                JsonArray chats = success.getLeft();
                for (int i = 0; i < right.size(); i++) {
                    Future<Message<JsonObject>> future = right.get(i);
                    if (future.succeeded()) {
                        JsonObject body = future.result().body();
                        chats.getJsonObject(i).put("event_summary", body.getString(ConstDef.REPLY_BASIS_DATA_KEYS.EVENT_SUMMARY));
                    } else {
                        chats.getJsonObject(i).put("event_summary", null);
                    }
                }
                JsonArray records = new JsonArray();

                for (int i = 0; i < chats.size(); i++) {
                    JsonObject jo = chats.getJsonObject(i);
                    String eventSummary = jo.getString("event_summary");
                    if (StringUtils.isNullOrEmpty(eventSummary))
                        continue;
                    String parentUid = jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);
                    JsonObject parentBasis = userBasisMap.get(parentUid);
                    Integer parentId = parentBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                    JsonObject parentInfo = userInfoMap.get(parentId);
                    JsonObject recordUserInfo = new JsonObject().put("name", parentBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME))
                            .put("avatar", parentBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR));
                    JsonObject record = new JsonObject().put("user_info", recordUserInfo)
                            .put("record_time", TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME)))
                            .put("record_title", "第" + (i + 1) + "次咨询")
                            .put("record_content", eventSummary);
                    records.add(record);
                }

                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", records).encodePrettily());
            }).onFailure(failure -> {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
            });
        }
    }



    private void delParentOrg(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }
        JsonObject reqBody = context.body().asJsonObject();
        int uid = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        reqBody.put(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_ID, uid);

        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.DELETE_ORG);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getJoinedOrg(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        Integer userId = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }

        List<Future<Message<JsonObject>>> futures = new ArrayList<>();
        List<Future<Message<JsonObject>>> orgExpertsFutures = new ArrayList<>();
        Promise<Pair<List<JsonObject>, List<Future<Message<JsonObject>>>>> p = Promise.promise();

        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_ORG_MEMBER_BY_MEMBER_ID);
        JsonObject para = new JsonObject().put(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_ID, userId);
        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para, d1).compose(v1 -> {
            JsonArray data = v1.body().getJsonArray("data");
            Promise<JsonArray> orgPromise = Promise.promise();
            if (data.isEmpty()) {
                orgPromise.complete(new JsonArray());
            } else {
                orgPromise.complete(data);
            }

            return orgPromise.future();
        }).compose(v2 -> {
            DeliveryOptions d2 = new DeliveryOptions();
            d2.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_ORG_BY_ID);
            for (int i = 0; i < v2.size(); i++) {
                Integer orgId = v2.getJsonObject(i).getInteger(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.ORGANIZATION_ID);
                JsonObject para2 = new JsonObject().put(ConstDef.ORGANIZATION_INFO_DATA_KEYS.ID, orgId);
                futures.add(context.vertx().eventBus().request(EventConst.USER.ID, para2, d2));
            }

            return Future.join(futures);
        }).compose(v3 -> {
            List<JsonObject> orgs = futures.stream().map(x -> x.result().body()).collect(Collectors.toList());
            for (JsonObject org : orgs) {
                Integer orgId = org.getInteger(ConstDef.ORGANIZATION_INFO_DATA_KEYS.ID);
                DeliveryOptions d3 = new DeliveryOptions();
                d3.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_ORG_MEMBER_BY_ORG_ID);
                JsonObject para3 = new JsonObject().put(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.ORGANIZATION_ID, orgId);
                orgExpertsFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para3, d3));
            }
            Future.join(orgExpertsFutures).onComplete(done -> p.complete(Pair.of(orgs, orgExpertsFutures)));

            return p.future();
        }).onSuccess(success -> {
            List<JsonObject> orgs = success.getLeft();
            for (int i = 0; i < orgExpertsFutures.size(); i++) {
                Future<Message<JsonObject>> future = orgExpertsFutures.get(i);
                if (future.succeeded()) {
                    JsonObject body = future.result().body();
                    int expertsInOrg = (int) body.getJsonArray("data")
                            .stream()
                            .map(o -> (JsonObject) o)
                            .filter(x -> x.getInteger(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_IDENTITY) == ConstDef.USER_IDENTITY.EXPERT)
                            .count();
                    orgs.get(i).put("expert_count", expertsInOrg);
                }
            }
            JsonObject rt = new JsonObject().put("organizations", orgs);
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }

    private void parentJoinOrg(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        Integer userId = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }
        JsonObject reqBody = context.body().asJsonObject();
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_ORG_BY_INVITE_CODE);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).compose(v1 -> {
            JsonArray data = v1.body().getJsonArray("data");
            Promise<JsonObject> orgPromise = Promise.promise();
            if (data.isEmpty()) {
                orgPromise.fail("没有该组织信息");
            } else {
                orgPromise.complete(data.getJsonObject(0));
            }

            return orgPromise.future();
        }).compose(v2 -> {
            DeliveryOptions d2 = new DeliveryOptions();
            d2.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.ADD_ORG_MEMBER);
            JsonObject para = new JsonObject().put(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.ORGANIZATION_ID, v2.getInteger(ConstDef.ORGANIZATION_INFO_DATA_KEYS.ID))
                    .put(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_ID, userId)
                    .put(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_IDENTITY, userIdentity);

            return context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para, d2);
        }).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            if ("没有该组织信息".equals(failure.getMessage()) || "已加入该组织".equals(failure.getMessage()))
                response.setStatusCode(HttpStatus.SC_OK);
            else
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void userPresenceNotify(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject reqBody = context.body().asJsonObject();
        String uid = reqBody.getString("userUid");
        String status = reqBody.getString("status");

        UserStatusCache.setUserStatus(redis, uid, status).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(),null).encodePrettily());
        });
    }


    private void getExpertInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        if (userIdentity != ConstDef.USER_IDENTITY.EXPERT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }

        JsonObject reqBody = new JsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.ID, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID));
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_ID);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).compose(v1 -> {
            JsonObject user = v1.body();
            Integer userId = user.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
            JsonObject para = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
            Promise<Pair<JsonObject, JsonObject>> promise = Promise.promise();
            context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para, d2).onSuccess(v2 -> {
                promise.complete(Pair.of(user, v2.body()));
            }).onFailure(failure -> {
                promise.fail(failure.getMessage());
            });

            return promise.future();
        }).onSuccess(success -> {
            JsonObject userBasis = success.getLeft();
            JsonObject expertInfo = success.getRight();
            JsonObject rt = new JsonObject()
                    .put("expert_name", userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME))
                    .put("expert_sex", userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_SEX))
                    .put("avatar_path", userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR))
                    .put("field", expertInfo.getString(ConstDef.EXPERT_INFO_DATA_KEYS.FIELD))
                    .put("level", expertInfo.getString(ConstDef.EXPERT_INFO_DATA_KEYS.LEVEL))
                    .put("seniority", expertInfo.getString(ConstDef.EXPERT_INFO_DATA_KEYS.SENIORITY));

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getParentInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        String parentUid = context.request().getParam("parent_id");
        JsonObject reqBody = new JsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, parentUid);
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).compose(v1 -> {
            JsonObject user = v1.body();
            Integer userId = user.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_PARENT_INFO_BY_USER_ID);
            JsonObject para = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
            Promise<Pair<JsonObject, JsonObject>> promise = Promise.promise();
            context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para, d2).onSuccess(v2 -> {
                promise.complete(Pair.of(user, v2.body()));
            }).onFailure(failure -> {
                promise.fail(failure.getMessage());
            });

            return promise.future();
        }).compose(v -> {
            JsonObject userBasis = v.getLeft();
            JsonObject parentInfo = v.getRight();

            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
            JsonObject para = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));
            Promise<Pair<JsonObject, JsonObject>> promise = Promise.promise();
            context.vertx().eventBus().<JsonObject>request(EventConst.COMMON.ID, para, d).onSuccess(v2 -> {
                JsonObject replyBasis = v2.body();
                if (replyBasis != null)
                    parentInfo.put(ConstDef.REPLY_BASIS_DATA_KEYS.PROFILE, replyBasis.getString(ConstDef.REPLY_BASIS_DATA_KEYS.PROFILE));
                promise.complete(Pair.of(userBasis, parentInfo));
            }).onFailure(failure -> {
                promise.fail(failure.getMessage());
            });

            return promise.future();
        }).compose(v3 -> {
            JsonObject userBasis = v3.getLeft();
            JsonObject parentInfo = v3.getRight();
            Integer parentId = parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);

            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_CHILD_INFO_BY_PARENT_ID);
            JsonObject para2 = new JsonObject().put(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID, parentId);

            DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_FAMILY_INFO_BY_PARENT_ID);
            JsonObject para3 = new JsonObject().put(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID, parentId);

            Promise<Tuple> promise = Promise.promise();

            Future<Message<JsonObject>> childFut = context.vertx().eventBus().request(EventConst.USER.ID, para2, d2);
            Future<Message<JsonObject>> familyFut = context.vertx().eventBus().request(EventConst.USER.ID, para3, d3);
            Future.join(childFut, familyFut).onComplete(done -> {
                if (done.succeeded()) {
                    JsonObject childInfo = childFut.result().body();
                    JsonObject familyInfo = familyFut.result().body();
                    promise.complete(Tuple.of(userBasis, parentInfo, childInfo, familyInfo));
                } else {
                    promise.fail(done.cause().getMessage());
                }
            });

            return promise.future();
        }).compose(v4 -> {
            Promise<Tuple> promise = Promise.promise();
            JsonObject para = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID, parentUid).put(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID, userUid).put("isFilterByDeleted", false);
            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL_BY_PARA);
            context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, para, d).onComplete(done -> {
                if (done.succeeded()) {
                    promise.complete(v4.addValue(done.result().body()));
                } else {
                    promise.fail(done.cause().getMessage());
                }
            });
            
            return promise.future();
        }).compose(v5 -> {
            JsonArray chats = v5.getJsonArray(4);
            List<Future<Message<JsonObject>>> chatReplyFutures = new ArrayList<>();
            Promise<Pair<Tuple, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
            for (int i = 0; i < chats.size(); i++) {
                String chatUuid = chats.getJsonObject(i).getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                DeliveryOptions d5 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
                JsonObject para5 = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, chatUuid);
                Future<Message<JsonObject>> chatReplyBasisFut = context.vertx().eventBus().request(EventConst.COMMON.ID, para5, d5);
                chatReplyFutures.add(chatReplyBasisFut);
            }
            Future.join(chatReplyFutures).onComplete(done -> promise.complete(Pair.of(v5, chatReplyFutures)));

            return promise.future();
        }).onSuccess(success -> {
            Tuple data = success.getLeft();
            List<Future<Message<JsonObject>>> right = success.getRight();
            JsonObject userBasis = data.getJsonObject(0);
            JsonObject parentInfo = data.getJsonObject(1);
            JsonArray childInfo = data.getJsonObject(2).getJsonArray("data");
            JsonObject familyInfo = data.getJsonObject(3);
            JsonArray chatRecords = data.getJsonArray(4);
            for (int i = 0; i < right.size(); i++) {
                Future<Message<JsonObject>> future = right.get(i);
                if (future.succeeded()) {
                    JsonObject body = future.result().body();
                    chatRecords.getJsonObject(i).put("event_summary", body.getString(ConstDef.REPLY_BASIS_DATA_KEYS.EVENT_SUMMARY));
                } else {
                    chatRecords.getJsonObject(i).put("event_summary", null);
                }
            }

            if (familyInfo != null && !familyInfo.isEmpty()) {
                familyInfo.remove(ConstDef.FAMILY_INFO_DATA_KEYS.ID);
                familyInfo.remove(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID);
            }

            if (parentInfo != null && !parentInfo.isEmpty()) {
                parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.ID);
                parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID);
                parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.PASSWORD);
            }

            childInfo.stream().map(obj -> (JsonObject) obj).forEach(jo -> {
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.ID);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID);
            });

            JsonObject parentData = new JsonObject()
                    .put("parent_name", userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME))
                    .put("parent_sex", userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_SEX))
                    .put("parent_age", userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_AGE))
                    .put("avatar_path", userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR));

            JsonArray records = new JsonArray();
            for (int i = 0; i < chatRecords.size(); i++) {
                JsonObject jo = chatRecords.getJsonObject(i);
                String eventSummary = jo.getString("event_summary");
                if (StringUtils.isNullOrEmpty(eventSummary))
                    continue;
                JsonObject record = new JsonObject().put("record_time", TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME)))
                        .put("record_title", "第" + (i + 1) + "次咨询")
                        .put("record_content", eventSummary);
                records.add(record);
            }

            JsonObject rt = new JsonObject()
                    .put("parent_info", parentData.mergeIn(parentInfo))
                    .put("children_info", childInfo)
                    .put("family_info", familyInfo)
                    .put("records", records);

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            log.debug("the getParentInfo failure: {}", failure.getMessage());
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void saveFamilyInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }
        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.ID, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID));
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.UPDATE_FAMILY);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void saveParentInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);

        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }

        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.ID, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID))
                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY, userIdentity);
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.UPDATE);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void saveExpertInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);

        if (userIdentity != ConstDef.USER_IDENTITY.EXPERT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }

        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.ID, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID))
                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY, userIdentity);
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.UPDATE);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void saveChildInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }
        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.ID, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID));
        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.UPDATE_CHILD);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).onSuccess(success -> {
            JsonObject body = success.body();
            JsonObject rt = new JsonObject().put("child_id", body.getString(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID));
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void delChildInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }
        JsonObject reqBody = context.body().asJsonObject();
        int uid = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.ID, uid);

        DeliveryOptions d1 = new DeliveryOptions();
        d1.addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.DELETE_CHILD);

        context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getCheckNo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.exceptionHandler(context.vertx(), response);
        HttpUtils.setHttpHeader(response);

        Boolean testSignin = SysConfigPara.conf.common_config.test_signin;

        String phone;
        String identity;
        try {
            phone = context.request().getParam("phone");
            identity = context.request().getParam("identity");
            if (StringUtils.isNullOrEmpty(phone) || StringUtils.isNullOrEmpty(identity))
                throw new InvalidParaException("null phone or identity not allowed");
            else if (Integer.parseInt(identity) != 0 && Integer.parseInt(identity) != 1)
                throw new InvalidParaException("invalid identity");
            boolean validPhoneNumber = CommonUtils.isValidPhoneNumber(phone, testSignin);
            if (!validPhoneNumber)
                throw new InvalidParaException("invalid phone number");
        } catch (Exception e) {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            response.end(ResponseUtils.response(HttpStatus.SC_BAD_REQUEST, e.getMessage(), null).encodePrettily());
            return;
        }

        String checkNo = testSignin ? "2024" : RandomStringUtils.randomNumeric(4);

        Promise<Void> promise1 = Promise.promise();
        if (testSignin) {
            promise1.complete();
        } else {
            WebClient webClient = WebClient.create(context.vertx());
            SmsUtils.sendMessage(webClient, phone, checkNo, promise1);
        }
        Promise<Void> promise2 = Promise.promise();
        UserCheckNoCache.setCheckNo(redis, phone, identity, checkNo, promise2);

        Future.all(promise1.future(), promise2.future()).onComplete(done -> {
            if (done.failed()) {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, done.cause().getMessage(), null).encodePrettily());
            } else {
                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "发送成功", null).encodePrettily());
            }
        });
    }
}


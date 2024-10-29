package com.misscut.utils;

import com.futureinteraction.utils.RedisHolder;
import com.misscut.cache.UserStatusCache;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;

import java.util.UUID;

/**
 * @Author WangWenTao
 * @Date 2024-08-20 19:21:46
 **/
@Slf4j
public class PushUtils {

    /**
     * 推送回复消息
     *
     * @param vertx
     * @param msgList
     */
    public static void pushAddedMsg(Vertx vertx, WebClient webClient, RedisHolder redis, JsonArray msgList, boolean isBatch) {
        log.trace("the pushed msgList: {}", msgList.encodePrettily());
        for (int i = 0; i < msgList.size(); i++) {
            JsonObject jo = msgList.getJsonObject(i);
            jo.put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.dateFormatTimestampNoISO(jo.getString(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME)));
        }
        if (isBatch) {
            String senderId = msgList.getJsonObject(0).getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
            String receiverId = msgList.getJsonObject(0).getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
            int receiverIdentity = msgList.getJsonObject(0).getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
            Promise<JsonObject> expertInfoPromise = Promise.promise();

            if ("system".equals(senderId)) {
                expertInfoPromise.complete();
            } else {
                DeliveryOptions d0 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                JsonObject para0 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, senderId);
                vertx.eventBus().<JsonObject>request(EventConst.USER.ID, para0, d0).onSuccess(v -> {
                    JsonObject body = v.body();
                    Integer userId = body.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                    DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
                    JsonObject para1 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                    vertx.eventBus().<JsonObject>request(EventConst.USER.ID, para1, d1).onComplete(done -> {
                        if (done.succeeded()) {
                            expertInfoPromise.complete(done.result().body());
                        } else {
                            expertInfoPromise.complete();
                        }
                    });
                }).onFailure(failure -> {
                    expertInfoPromise.fail(failure.getMessage());
                });
            }
            expertInfoPromise.future().onSuccess(success -> {
                UserStatusCache.getUserStatus(redis, receiverId).compose(v -> {
                    Promise<String> p = Promise.promise();
                    if (v == null) {
                        WebUtils.queryUserStatus(webClient, receiverIdentity, receiverId, p);
                    } else {
                        p.complete(v.toString());
                    }

                    return p.future();
                }).onSuccess(v2 -> {
                    if ("online".equals(v2)) {
                        String batchId = UUID.randomUUID().toString();
                        for (int i = 0; i < msgList.size(); i++) {
                            JsonObject msg = msgList.getJsonObject(i);
                            msg.mergeIn(new JsonObject().put("expert_info", success));
                            msg.put("sequence", i + 1);
                        }
                        JsonObject pushData = CommonUtils.genPushData(ConstDef.REPLY_CMD.CREATE_MSG_REPLY, msgList, new JsonArray().add(receiverId), receiverIdentity, batchId);
                        WebUtils.doPush(webClient, pushData, "p2p")
                                .onSuccess(v3 -> log.debug("push ADD_MSG succeed: {}", v3.toJsonObject()))
                                .onFailure(failure -> log.error("push ADD_MSG failure: {}", failure.getMessage()));
                    }
                }).onFailure(failure -> {
                    log.error("get receiver status failure: {}, {}, {}", receiverId, receiverIdentity, failure.getMessage());
                });

            }).onFailure(failure -> {
                log.error("[pushAddedMsg] get expert info failure: {}", failure.getMessage());
            });
        } else {
            for (int i = 0; i < msgList.size(); i++) {
                JsonObject jo = msgList.getJsonObject(i);
                String receiverId = jo.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                int receiverIdentity = jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
                UserStatusCache.getUserStatus(redis, receiverId).compose(v -> {
                    Promise<String> p = Promise.promise();
                    if (v == null) {
                        WebUtils.queryUserStatus(webClient, receiverIdentity, receiverId, p);
                    } else {
                        p.complete(v.toString());
                    }

                    return p.future();
                }).onSuccess(v2 -> {
                    if ("online".equals(v2)) {
                        String batchId = UUID.randomUUID().toString();
                        JsonObject pushData = CommonUtils.genPushData(ConstDef.REPLY_CMD.CREATE_MSG_REPLY, new JsonArray().add(jo), new JsonArray().add(receiverId), receiverIdentity, batchId);
                        WebUtils.doPush(webClient, pushData, "p2p")
                                .onSuccess(v3 -> log.debug("push ADD_MSG succeed: {}", v3.toJsonObject()))
                                .onFailure(failure -> log.error("push ADD_MSG failure: {}", failure.getMessage()));
                    }
                }).onFailure(failure -> {
                    log.error("get receiver status failure: {}, {}, {}", receiverId, receiverIdentity, failure.getMessage());
                });
            }
        }
    }

    /**
     * 对话接收者更新为专家并且为专家推送此chat
     *
     * @param vertx
     * @param userInfo
     * @param chatUuid
     * @param expertId
     */
    public static void updatePushChatAfterRecommendExpert(Vertx vertx, WebClient webClient, RedisHolder redis, JsonObject userInfo, String chatUuid, String expertId) {
        JsonObject para = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, chatUuid)
                .put(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID, expertId)
                .put(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY, ConstDef.USER_IDENTITY.EXPERT);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.UPDATE);
        vertx.eventBus().<JsonObject>request(EventConst.CHAT.ID, para, d1).onSuccess(success -> {
            JsonObject body = success.body();
            body.put("sender_name", userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME));
            body.put("sender_avatar_path", userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR));

            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
            JsonObject para1 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, expertId);
            vertx.eventBus().<JsonObject>request(EventConst.USER.ID, para1, d2).compose(v -> {
                Promise<JsonObject> promise = Promise.promise();
                JsonObject userBasis = v.body();
                Integer userId = userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                String userName = userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME);
                String avatarPath = userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR);
                JsonObject res = new JsonObject().put("receiver_name", userName)
                        .put("receiver_avatar_path", avatarPath);
                DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
                JsonObject para3 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                vertx.eventBus().<JsonObject>request(EventConst.USER.ID, para3, d3).onComplete(done -> {
                    if (done.succeeded()) {
                        String field = done.result().body().getString(ConstDef.EXPERT_INFO_DATA_KEYS.FIELD);
                        res.put("expert_field", field);
                    }
                    promise.complete(res);
                });

                return promise.future();
            }).onSuccess(v1 -> {
                JsonObject rt = body.mergeIn(v1);
                UserStatusCache.getUserStatus(redis, expertId).compose(v -> {
                    Promise<String> p = Promise.promise();
                    if (v == null) {
                        WebUtils.queryUserStatus(webClient, ConstDef.USER_IDENTITY.EXPERT, expertId, p);
                    } else {
                        p.complete(v.toString());
                    }

                    return p.future();
                }).onSuccess(v2 -> {
                    if ("online".equals(v2)) {
                        String batchId = UUID.randomUUID().toString();
                        JsonObject pushData = CommonUtils.genPushData(ConstDef.REPLY_CMD.ADD_CHAT, new JsonArray().add(rt), new JsonArray().add(expertId), ConstDef.USER_IDENTITY.EXPERT, batchId);
                        WebUtils.doPush(webClient, pushData, "p2p")
                                .onSuccess(v3 -> log.debug("push ADD_CHAT succeed: {}", v3.toJsonObject()))
                                .onFailure(failure -> log.error("push ADD_CHAT failure: {}", failure.getMessage()));
                    }
                }).onFailure(failure -> {
                    log.error("get receiver status failure: {}, {}, {}", expertId, ConstDef.USER_IDENTITY.EXPERT, failure.getMessage());
                });
            }).onFailure(failure -> {
                log.error("query chat related user info failure: {}", failure.getMessage());
            });
        }).onFailure(failure -> {
            log.error("update chat receiver failure: {}, {}, {}", chatUuid, expertId, failure.getMessage());
        });
    }


    /**
     * 推送更新的chat信息
     * @param webClient
     * @param redis
     * @param pusherId
     * @param pusherIdentity
     * @param chatInfo
     */
    public static void pushChatInfo(WebClient webClient, RedisHolder redis, String pusherId, int pusherIdentity, JsonObject chatInfo) {
        UserStatusCache.getUserStatus(redis, pusherId).compose(v -> {
            Promise<String> p = Promise.promise();
            if (v == null) {
                WebUtils.queryUserStatus(webClient, pusherIdentity, pusherId, p);
            } else {
                p.complete(v.toString());
            }

            return p.future();
        }).onSuccess(v -> {
            if ("online".equals(v)) {
                String batchId = UUID.randomUUID().toString();
                JsonObject pushData = CommonUtils.genPushData(ConstDef.REPLY_CMD.UPDATE_CHAT, new JsonArray().add(chatInfo), new JsonArray().add(pusherId), pusherIdentity, batchId);
                WebUtils.doPush(webClient, pushData, "p2p")
                        .onSuccess(v2 -> log.debug("push UPDATE_CHAT succeed: {}", v2.toJsonObject()))
                        .onFailure(failure -> log.error("push UPDATE_CHAT failure: {}", failure.getMessage()));
            }
        }).onFailure(failure -> {
            log.error("get receiver status failure: {}, {}, {}", pusherId, pusherIdentity, failure.getMessage());
        });
    }
}

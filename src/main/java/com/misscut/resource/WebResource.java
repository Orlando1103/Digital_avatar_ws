package com.misscut.resource;

import com.futureinteraction.utils.HttpUtils;
import com.futureinteraction.utils.RedisHolder;
import com.futureinteraction.utils.StringUtils;
import com.google.gson.Gson;
import com.misscut.cache.UserStatusCache;
import com.misscut.config.SysConfigPara;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.utils.*;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.HttpStatus;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author wangwentao
 * @date 2024/03/18 上午8:58
 */
@Slf4j
public class WebResource {

    private Gson gson = new Gson();

    private RedisHolder redis;
    private WebClient webClient;

    String PATH = "/web";

    // 注册路由
    public void register(Vertx vertx, Router mainRouter, Router router, RedisHolder redis) {
        this.redis = redis;
        this.webClient = WebClient.create(vertx);

        router.get("/random_question").handler(this::getRandomQuestion);
        router.post("/create_chat").handler(this::createChat);
        router.get("/get_chats").handler(this::getChats);

        router.get("/get_messages").handler(this::getMessages);
        router.post("/delete_chat").handler(this::deleteChatByUuid);
        router.post("/update_chat").handler(this::updateChat);
        router.post("/delete_message").handler(this::deleteMsg);
        router.post("/update/msg_info").handler(this::updateMsgInfo);
        router.get("/get_chat_resource").handler(this::getChatResource);
        router.post("/delete_chat_resource").handler(this::deleteChatResource);
        router.get("/get_favorites").handler(this::getFavorites);
        router.get("/get_consult_statistics").handler(this::getConsult);

        //家长退出聊天时主动更新标题
        router.post("/trigger/update_chat_title").handler(this::triggerUpdateChatTitle);

        router.get("/get_favorites_details").handler(this::getFavoriteDetails);
        router.get("/download_resource").handler(this::downloadResource);
        router.get("/get_message_info").handler(this::getMessageInfo);

        // 提供给算法的接口
        router.post("/mal/save_logic").handler(this::saveLogic);
        router.get("/mal/get_logic").handler(this::getLogic);

        // 提供给websocket
        router.get("/ws/get_message").handler(this::getMessageFromWs);
        router.post("/ws/create_message").handler(this::createMessageFromWs);
        router.post("/ws/update_message").handler(this::updateMessageFromWs);
        router.post("/ws/handle_ai_assistant").handler(this::handleAiAssistant);

        mainRouter.mountSubRouter(PATH, router);
        HttpUtils.dumpRestApi(router, PATH, log);
    }


    private void getMessageInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);
        JsonObject userInfo = context.user().principal();

        String id = context.request().getParam("message_uuid");
        DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_MSG_INFO_BY_MSG_UUID);
        JsonObject para = new JsonObject().put(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID, id);

        context.vertx().eventBus().<JsonObject>request(EventConst.MESSAGE.ID, para, d).onSuccess(success -> {
            JsonObject body = success.body();
            String audioPath = body.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_PATH);
            if (StringUtils.isNullOrEmpty(audioPath)) {
                response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
                response.end(ResponseUtils.response(HttpStatus.SC_BAD_REQUEST, "暂无资源", null).encodePrettily());
            } else {
                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", new JsonObject().put("audio_path", audioPath)).encodePrettily());
            }
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void downloadResource(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        Vertx vertx = context.vertx();
        String id = context.request().getParam("id");

        JsonObject reqPara = new JsonObject();
        reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_file)
                .put(EventConst.ALGO_API.KEYS.PARA, new JsonObject().put("filepath", id))
                .put(EventConst.ALGO_API.KEYS.STREAM, true);

        vertx.eventBus().<Buffer>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                .onFailure(failure -> {
                    log.error("downloadResource from algo failure: {}, {}", id, failure.getMessage());
                })
                .onSuccess(v1 -> {
                    response.setStatusCode(HttpStatus.SC_OK);
                    response.end(v1.body());
                });
    }

    private void handleAiAssistant(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        Vertx vertx = context.vertx();
        JsonObject reqBody = context.body().asJsonObject();
        String chatUuid = reqBody.getString("chat_uuid");
        String messageUuid = reqBody.getString("message_uuid");

        JsonObject para1 = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, chatUuid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_BY_CHAT_UUID);
        context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, para1, d1).compose(v1 -> {
            JsonObject body = v1.body();
            String senderId = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
            DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
            JsonObject para4 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, senderId);

            return context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para4, d4);
        }).compose(v2 -> {
            JsonObject body = v2.body();
            Integer userId = body.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
            JsonObject queryParentPara = new JsonObject().put(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID, userId);
            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_PARENT_INFO_BY_USER_ID);
            Promise<Pair<JsonObject, JsonObject>> promise = Promise.promise();
            vertx.eventBus().<JsonObject>request(EventConst.USER.ID, queryParentPara, d)
                    .onSuccess(success -> promise.complete(Pair.of(body, success.body())))
                    .onFailure(promise::fail);

            return promise.future();
        }).compose(v -> {
            JsonObject parentInfo = v.getRight();
            JsonObject userBasis = v.getLeft();
            Integer parentId = parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);

            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_CHILD_INFO_BY_PARENT_ID);
            JsonObject para2 = new JsonObject().put(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID, parentId);

            DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_FAMILY_INFO_BY_PARENT_ID);
            JsonObject para3 = new JsonObject().put(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID, parentId);

            DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID);
            JsonObject para4 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid).put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));

            DeliveryOptions d5 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
            JsonObject para5 = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));

            DeliveryOptions d6 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC_KEY.ACTIONS.QUERY_ALL);
            JsonObject para6 = new JsonObject();

            Promise<Tuple> promise = Promise.promise();

            Future<Message<JsonObject>> childFut = vertx.eventBus().request(EventConst.USER.ID, para2, d2);
            Future<Message<JsonObject>> familyFut = vertx.eventBus().request(EventConst.USER.ID, para3, d3);
            Future<Message<JsonArray>> messagesFut = vertx.eventBus().request(EventConst.MESSAGE.ID, para4, d4);
            Future<Message<JsonObject>> parentReplyBasisFut = vertx.eventBus().request(EventConst.COMMON.ID, para5, d5);
            Future<Message<JsonArray>> logicKeyFut = vertx.eventBus().request(EventConst.COMMON.ID, para6, d6);

            Future.join(childFut, familyFut, messagesFut, parentReplyBasisFut, logicKeyFut).onComplete(done -> {
                if (done.succeeded()) {
                    JsonObject childInfo = childFut.result().body();
                    JsonObject familyInfo = familyFut.result().body();
                    JsonArray messages = messagesFut.result().body();
                    JsonObject replyBasis = parentReplyBasisFut.result().body();
                    JsonArray logicKeys = logicKeyFut.result().body();
                    promise.complete(Tuple.of(parentInfo, childInfo, familyInfo, messages, replyBasis, logicKeys));
                } else {
                    promise.fail(done.cause().getMessage());
                }
            });

            return promise.future();
        }).compose(success -> {
            JsonObject parentInfo = success.getJsonObject(0);
            JsonObject childInfo = success.getJsonObject(1);
            JsonObject familyInfo = success.getJsonObject(2);
            JsonArray messages = success.getJsonArray(3);
            JsonObject replyBasis = success.getJsonObject(4);
            JsonArray logicKeys = success.getJsonArray(5);

            JsonArray messagePara = new JsonArray();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject jo = messages.getJsonObject(i);
                messagePara.add(new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                        .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, jo.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT)));
            }

            JsonArray logicKeyPara = new JsonArray();
            for (int i = 0; i < logicKeys.size(); i++) {
                JsonObject jo = logicKeys.getJsonObject(i);
                logicKeyPara.add(new JsonObject().put("logic_key_id", jo.getInteger(ConstDef.LOGIC_KEY_DATA_KEYS.ID))
                        .put("logic_key", jo.getString(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT)));
            }

            JsonArray children = childInfo.getJsonArray("data");
            for (int i = 0; i < children.size(); i++) {
                JsonObject jo = children.getJsonObject(i);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.ID);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID);
            }

            parentInfo.mergeIn(replyBasis);
            parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.ID);
            parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID);
            parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.PASSWORD);

            familyInfo.remove(ConstDef.FAMILY_INFO_DATA_KEYS.ID);
            familyInfo.remove(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID);

            JsonObject requestBody = new JsonObject()
                    .put("current_content", null)
                    .put("messages", messagePara)
                    .put("parent_info", parentInfo)
                    .put("children_info", children)
                    .put("family_info", familyInfo)
                    .put("knowledge_base_uuid", chatUuid + "_" + parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID))
                    .put("all_logic_keys", logicKeyPara);

            Promise<JsonObject> malResPromise = Promise.promise();

            JsonObject reqPara = new JsonObject();
            reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_ai_reply)
                    .put(EventConst.ALGO_API.KEYS.PARA, requestBody);

            vertx.eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                    .onFailure(malResPromise::fail)
                    .onSuccess(v1 -> malResPromise.complete(v1.body()));

            return malResPromise.future();
        }).onSuccess(success -> {
            String allReply = success.getString("all_reply").replaceAll("\\n", "").replaceAll("#", "");
            JsonObject rt = new JsonObject().put("assistant_res", allReply).put("message_uuid", messageUuid);
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getFavoriteDetails(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        String chatUuid = context.request().getParam("chat_uuid");
        JsonObject para1 = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, chatUuid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_BY_CHAT_UUID);
        context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, para1, d1).compose(v1 -> {
            JsonObject chat = v1.body();
            List<Future<Message<JsonArray>>> futures = new ArrayList<>();
            JsonObject para2 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid).put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID);
            Future<Message<JsonArray>> messagesFuture = context.vertx().eventBus().request(EventConst.MESSAGE.ID, para2, d2);
            futures.add(messagesFuture);
            Promise<Tuple> promise = Promise.promise();
            Future.join(futures).onComplete(done -> {
                if (done.succeeded()) {
                    JsonArray body = messagesFuture.result().body();
                    promise.complete(Tuple.of(chat, body));
                } else {
                    promise.fail(done.cause().getMessage());
                }
            });

            return promise.future();
        }).compose(v2 -> {
            Promise<Tuple> promise = Promise.promise();
            JsonObject chat = v2.getJsonObject(0);
            DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
            JsonObject para4 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID));

            DeliveryOptions d5 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
            JsonObject para5 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID));

            Future<Message<JsonObject>> parentBasisFuture = context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para4, d4);
            Future<Message<JsonObject>> expertBasisFuture = context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para5, d5);

            Future.join(parentBasisFuture, expertBasisFuture).onComplete(done -> {
                if (done.succeeded()) {
                    JsonObject parentBasis = parentBasisFuture.result().body();
                    JsonObject expertBasis = expertBasisFuture.result().body();
                    v2.addValue(parentBasis);
                    v2.addValue(expertBasis);
                    promise.complete(v2);
                } else {
                    promise.fail(done.cause().getMessage());
                }
            });

            return promise.future();
        }).compose(v4 -> {
            JsonObject parentBasis = v4.getJsonObject(2);
            if (parentBasis == null)
                return Future.succeededFuture(v4);
            Promise<Tuple> promise = Promise.promise();
            Integer userId = parentBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
            DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_PARENT_INFO_BY_USER_ID);
            JsonObject para4 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
            context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para4, d4).onSuccess(v5 -> {
                promise.complete(v4.addValue(v5.body()));
            }).onFailure(failure -> {
                promise.fail(failure.getMessage());
            });

            return promise.future();
        }).onSuccess(success -> {
            JsonObject chat = success.getJsonObject(0);
            JsonArray messages = success.getJsonArray(1);
            JsonObject parentBasis = success.getJsonObject(2);
            JsonObject expertBasis = success.getJsonObject(3);
            JsonObject parentInfo = success.getJsonObject(4);

            String parentName = parentBasis != null ? parentBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME) : null;
            String expertName = expertBasis != null ? expertBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME) : null;
            String tag = parentInfo != null ? parentInfo.getString(ConstDef.PARENT_INFO_DATA_KEYS.TAG) : null;
            String chatCreateTime = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME);

            JsonArray aiMessageRes = new JsonArray();
            JsonArray expertMessageRes = new JsonArray();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject aiData = new JsonObject();
                JsonObject expertData = new JsonObject();
                JsonObject msg = messages.getJsonObject(i);
                log.trace("the msg: {}", msg.encode());
                String senderId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
                String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                Integer senderIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY);
                Integer receiverIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);

                if (!"system".equals(senderId) && !"system".equals(receiverId) && senderIdentity != ConstDef.USER_IDENTITY.BOT && receiverIdentity != ConstDef.USER_IDENTITY.BOT) {
                    if (msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY) == ConstDef.USER_IDENTITY.PARENT) {
                        expertData.put("name", parentName).put("message_content", msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT));
                    } else {
                        expertData.put("name", expertName).put("message_content", msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT));
                    }
                } else {
                    if ("system".equals(senderId) || senderIdentity == ConstDef.USER_IDENTITY.BOT) {
                        aiData.put("name", "AI").put("message_content", msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT));
                    } else {
                        aiData.put("name", parentName).put("message_content", msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT));
                    }
                }
                if (!aiData.isEmpty())
                    aiMessageRes.add(aiData);
                if (!expertData.isEmpty())
                    expertMessageRes.add(expertData);
            }
            JsonObject rt = new JsonObject()
                    .put("chat_create_time", TimeUtils.dateFormatTimestamp(chatCreateTime))
                    .put("tag", tag)
                    .put("messages", new JsonObject().put("ai_messages", aiMessageRes).put("expert_messages", expertMessageRes));

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void triggerUpdateChatTitle(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }

        JsonObject body = context.body().asJsonObject();
        DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_BY_CHAT_UUID);
        context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, body, d).compose(success -> {
            JsonObject data = success.body();
            String chatTitle = data.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE);
            Promise<JsonArray> promise = Promise.promise();
            if (StringUtils.isNullOrEmpty(chatTitle) || "chat".equals(chatTitle)) {
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID);
                JsonObject para2 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID)).put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
                context.vertx().eventBus().<JsonArray>request(EventConst.MESSAGE.ID, para2, d2).onSuccess(v -> {
                    JsonArray messages = v.body();
                    JsonArray messagePara = new JsonArray();
                    for (int i = 0; i < messages.size(); i++) {
                        JsonObject jo = messages.getJsonObject(i);
                        messagePara.add(new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, jo.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT)));
                    }
                    promise.complete(messagePara);
                }).onFailure(promise::fail);
            } else {
                promise.fail("no need to update chat title: " + body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID));
            }

            return promise.future();
        }).compose(v -> {
            Promise<JsonObject> promise = Promise.promise();
            JsonObject para = new JsonObject();
            para.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_chat_title)
                    .put(EventConst.ALGO_API.KEYS.PARA, new JsonObject().put("messages", v));

            context.vertx().eventBus().<JsonObject>request(EventConst.ALGO_API.ID, para, new DeliveryOptions().setSendTimeout(60 * 1000))
                    .onFailure(promise::fail)
                    .onSuccess(v1 -> promise.complete(v1.body()));

            return promise.future();
        }).compose(v -> {
            v.mergeIn(body);
            DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.UPDATE);
            return context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, v, d1);
        }).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());

            JsonObject chat = success.body();

            JsonObject rt = new JsonObject().put("chat_uuid", chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID))
                    .put("chat_title", chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE));
            PushUtils.pushChatInfo(webClient, redis, userUid, userIdentity, rt);

        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getLogic(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        String id = context.request().getParam("id");

        JsonObject para = new JsonObject().put(ConstDef.LOGIC_DATA_KEYS.LOGIC_KEY_ID, Integer.valueOf(id));
        DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC.ACTIONS.QUERY_BY_LOGIC_KEY_ID);
        context.vertx().eventBus().<JsonArray>request(EventConst.COMMON.ID, para, d).onSuccess(success -> {
            JsonArray body = success.body();
            for (int i = 0; i < body.size(); i++) {
                JsonObject jo = body.getJsonObject(i);
                jo.remove(ConstDef.LOGIC_DATA_KEYS.ID);
                jo.remove(ConstDef.LOGIC_DATA_KEYS.LOGIC_KEY_ID);
            }

            JsonObject rt = new JsonObject().put("logics", body);
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });

    }


    private void saveLogic(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject reqBody = context.body().asJsonObject();
        Integer logicKeyId = reqBody.getInteger("logic_key_id");
        String logicKey = reqBody.getString("logic_key");
        JsonObject logics = reqBody.getJsonObject("logics");

        if (logicKey.isEmpty()) {
            response.setStatusCode(HttpStatus.SC_BAD_REQUEST);
            response.end(ResponseUtils.response(HttpStatus.SC_BAD_REQUEST, "logic_key can not empty", null).encodePrettily());
            return;
        }

        Promise<Boolean> addLogicKeyPromise = Promise.promise();
        if (logicKeyId == null) {
            addLogicKeyPromise.complete(true);
        } else {
            JsonObject para = new JsonObject().put(ConstDef.LOGIC_KEY_DATA_KEYS.ID, logicKeyId).put(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT, logicKey);
            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC_KEY.ACTIONS.QUERY_ALL);
            context.vertx().eventBus().<JsonArray>request(EventConst.COMMON.ID, para, d).onSuccess(success -> {
                JsonArray body = success.body();
                if (!body.isEmpty()) {
                    addLogicKeyPromise.complete(false);
                } else {
                    addLogicKeyPromise.fail("Bad Param");
                }
            }).onFailure(addLogicKeyPromise::fail);
        }
        addLogicKeyPromise.future().compose(success -> {
            Promise<Void> promise = Promise.promise();
            if (success) {
                JsonObject para = new JsonObject().put(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT, logicKey);
                para.mergeIn(logics);
                DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC_KEY.ACTIONS.ADD_LOGIC_KEY);
                context.vertx().eventBus().<JsonObject>request(EventConst.COMMON.ID, para, d)
                        .onSuccess(v -> promise.complete())
                        .onFailure(promise::fail);
            } else {
                logics.put(ConstDef.LOGIC_DATA_KEYS.LOGIC_KEY_ID, logicKeyId);
                DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC.ACTIONS.ADD_LOGIC);
                context.vertx().eventBus().<JsonObject>request(EventConst.COMMON.ID, logics, d)
                        .onSuccess(v -> promise.complete())
                        .onFailure(promise::fail);
            }

            return promise.future();
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        }).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        });
    }


    private void getConsult(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        JsonObject reqBody = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL);
        context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, reqBody, d1).onSuccess(success -> {
            JsonArray body = success.body();
            int aiChats = 0;
            int intervenedChats = 0;
            int waitChats = 0;
            for (int i = 0; i < body.size(); i++) {
                Integer chatStatus = body.getJsonObject(i).getInteger(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_STATUS);
                if (chatStatus == ConstDef.CHAT_STATUS.AI)
                    aiChats++;
                if (chatStatus == ConstDef.CHAT_STATUS.EXPERT_TO_INTERVENE)
                    intervenedChats++;
                if (chatStatus == ConstDef.CHAT_STATUS.EXPERT_ALREADY_INTERVENED)
                    waitChats++;
            }

            JsonObject rt = new JsonObject()
                    .put("all_chats", body.size())
                    .put("ai_chats", aiChats)
                    .put("intervened_chats", intervenedChats)
                    .put("wait_chats", waitChats);

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getFavorites(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        JsonObject reqBody = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_STATUS, ConstDef.CHAT_IS_FAVORITE.COLLECT)
                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);

        Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
        List<Future<Message<JsonObject>>> futures = new ArrayList<>();
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL);
        context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, reqBody, d1).compose(success -> {
            JsonArray body = success.body();
            JsonArray rt = new JsonArray();
            for (int i = 0; i < body.size(); i++) {
                JsonObject chat = body.getJsonObject(i);
                JsonObject data = new JsonObject().put("chat_uuid", chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID))
                        .put("favorite_time", chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_TIME))
                        .put("chat_title", chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE));
                rt.add(data);
                String chatUuid = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_LATEST_MSG_BY_CHAT_UUID_AND_SENDER);
                futures.add(context.vertx().eventBus().request(EventConst.MESSAGE.ID, new JsonObject().put("chat_uuid", chatUuid), d2));
            }
            Future.join(futures).onComplete(done -> promise.complete(Pair.of(rt, futures)));

            return promise.future();
        }).onSuccess(success -> {
            JsonArray rt = success.getLeft();
            for (int i = 0; i < rt.size(); i++) {
                if (futures.get(i).succeeded()) {
                    rt.getJsonObject(i).mergeIn(futures.get(i).result().body());
                }
            }
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void deleteChatResource(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject reqBody = context.body().asJsonObject();
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.DELETE_RESOURCE);
        context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getChatResource(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        String chatUuid = context.request().getParam("chat_uuid");

        JsonObject para = new JsonObject();
        para.put(ConstDef.CHAT_RESOURCE.CHAT_UUID, chatUuid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_RESOURCE);
        context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, para, d1).onSuccess(success -> {
            JsonArray data = success.body();
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", data).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void updateMsgInfo(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();

        JsonObject reqBody = context.body().asJsonObject();
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.UPDATE_MSG_INFO);
        context.vertx().eventBus().<JsonObject>request(EventConst.MESSAGE.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());

            JsonObject body = success.body();
            JsonObject msg = body.getJsonObject("msg");
            JsonObject msgInfo = body.getJsonObject("msgInfo");

            if (reqBody.containsKey(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_REVISION)) {
                String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                Integer receiverIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
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
                        JsonObject pushData = CommonUtils.genPushData(ConstDef.REPLY_CMD.MSG_EDIT, new JsonArray().add(reqBody), new JsonArray().add(receiverId), receiverIdentity, batchId);
                        WebUtils.doPush(webClient, pushData, "p2p")
                                .onSuccess(v3 -> log.debug("push ADD_MSG succeed: {}", v3.toJsonObject()))
                                .onFailure(failure -> log.error("push ADD_MSG failure: {}", failure.getMessage()));
                    }
                }).onFailure(failure -> {
                    log.error("get receiver status failure: {}, {}, {}", receiverId, receiverIdentity, failure.getMessage());
                });
            }

            if (reqBody.containsKey(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_SCORE) || reqBody.containsKey(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_FEEDBACK)) {
                String chatUuid = msg.getString(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID);
                JsonObject para = new JsonObject().put("chat_uuid", chatUuid).put("topN", 5);

                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.CHAT_USED_KNOWLEDGE.ACTIONS.QUERY_TOP_N_BY_CHAT_UUID);
                // 1. 生成推荐资源消息
                context.vertx().eventBus().<JsonArray>request(EventConst.COMMON.ID, para, d2).compose(v -> {
                    JsonArray knowledgeUuids = v.body();
                    JsonArray malPara = new JsonArray();
                    for (int i = 0; i < knowledgeUuids.size(); i++) {
                        malPara.add(knowledgeUuids.getJsonObject(i).getString(ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.KNOWLEDGE_UUID));
                    }

                    Promise<JsonObject> promise = Promise.promise();
                    JsonObject reqPara = new JsonObject();
                    reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_chat_resource_info)
                            .put(EventConst.ALGO_API.KEYS.PARA, new JsonObject().put("knowledge_uuids", malPara).put("knowledge_base_uuid", chatUuid + "_" + userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID)));

                    log.trace("to get chat resourceInfo para is: {}", reqPara.encodePrettily());
                    context.vertx().eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                            .onFailure(promise::fail)
                            .onSuccess(v1 -> promise.complete(v1.body()));

                    return promise.future();
                }).compose(v -> {
                    JsonArray resources = v.getJsonArray("resources");
                    if (resources == null || resources.isEmpty())
                        return Future.failedFuture("algo recommend is empty");
                    for (int i = 0; i < resources.size(); i++) {
                        JsonObject jo = resources.getJsonObject(i);
                        jo.put(ConstDef.CHAT_RESOURCE.CHAT_UUID, chatUuid);
                        jo.put(ConstDef.CHAT_RESOURCE.CREATE_TIME, TimeUtils.getTime());
                    }
                    Promise<JsonArray> p = Promise.promise();
                    context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, new JsonObject().put("resourceList", resources),
                                    new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.ADD_BATCH_CHAT_RESOURCE))
                            .onSuccess(v1 -> p.complete(v1.body()))
                            .onFailure(failure -> p.fail(failure.getMessage()));

                    return p.future();
                }).compose(resources -> {
                    JsonArray recommendMsgArray = new JsonArray();
                    JsonArray recommendMsgPushData = new JsonArray();
                    //推荐提示消息
                    JsonObject message = new JsonObject()
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, UUID.randomUUID() + "-msg")
                            .put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid)
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, ConstDef.USER_IDENTITY.BOT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID, "system")
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID))
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY))
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE, ConstDef.MSG_TYPE.SYSTEM)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CATEGORY, ConstDef.MSG_CATEGORY.TEXT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, "感谢您的评分！为了更好地帮助您，专家为您推荐家庭教育相关的案例资源。")
                            .put(ConstDef.MESSAGE_DATA_KEYS.MACHINE_SCORE, null)
                            .put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.getTime());

                    recommendMsgArray.add(message);
                    recommendMsgPushData.add(message);
                    //推荐资源卡片消息
                    for (int i = 0; i < resources.size(); i++) {
                        JsonObject jo = resources.getJsonObject(i);
                        Integer resourceId = jo.getInteger(ConstDef.CHAT_RESOURCE.ID);
                        Integer resourceType = jo.getInteger(ConstDef.CHAT_RESOURCE.RESOURCE_TYPE);
                        JsonObject cardMsg = new JsonObject()
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, UUID.randomUUID() + "-msg")
                                .put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid)
                                .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, ConstDef.USER_IDENTITY.BOT)
                                .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID, "system")
                                .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID))
                                .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY))
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE, ConstDef.MSG_TYPE.CARD)
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING)
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CATEGORY, ConstDef.MSG_CATEGORY.TEXT)
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, "")
                                .put(ConstDef.MESSAGE_DATA_KEYS.MACHINE_SCORE, null)
                                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.CHAT_RESOURCE_ID, resourceId)
                                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.CARD_TYPE, resourceType);

                        recommendMsgArray.add(cardMsg);
                        String resourceUrl = resourceType == ConstDef.CHAT_RESOURCE_TYPE.WEB ? jo.getString(ConstDef.CHAT_RESOURCE.RESOURCE_URL) : jo.getString(ConstDef.CHAT_RESOURCE.RESOURCE_PATH);
                        JsonObject pushResourceInfo = new JsonObject().put("resource_id", jo.getInteger(ConstDef.CHAT_RESOURCE.ID))
                                .put("title", jo.getString(ConstDef.CHAT_RESOURCE.TITLE))
                                .put("source", jo.getString(ConstDef.CHAT_RESOURCE.SOURCE))
                                .put("resource_url", resourceUrl);

                        recommendMsgPushData.add(new JsonObject(cardMsg.encode()).put("resource_info", pushResourceInfo));
                    }

                    Promise<JsonArray> p = Promise.promise();
                    context.vertx().eventBus().<JsonObject>request(EventConst.MESSAGE.ID, new JsonObject().put("msgList", recommendMsgArray),
                                    new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.ADD_BATCH))
                            .onSuccess(v -> p.complete(recommendMsgPushData))
                            .onFailure(failure -> p.fail(failure.getMessage()));

                    return p.future();
                }).onSuccess(messages -> {
                    PushUtils.pushAddedMsg(context.vertx(), webClient, redis, messages, true);
                }).onFailure(failure -> {
                    log.error("generate recommend msg failure: {}, {}", chatUuid, failure.getMessage());
                });

                // 2.总结逻辑链
                DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID);
                JsonObject para3 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid).put("isFilterByDeleted", false);

                DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC_KEY.ACTIONS.QUERY_ALL);
                JsonObject para4 = new JsonObject();

                DeliveryOptions d7 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL);
                JsonObject para7 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));

                Future<Message<JsonArray>> messagesFut = context.vertx().eventBus().request(EventConst.MESSAGE.ID, para3, d3);
                Future<Message<JsonArray>> logicKeyFut = context.vertx().eventBus().request(EventConst.COMMON.ID, para4, d4);
                Future<Message<JsonArray>> chatsFut = context.vertx().eventBus().request(EventConst.CHAT.ID, para7, d7);

                Future.join(messagesFut, logicKeyFut).onComplete(done -> {
                    if (done.succeeded()) {
                        JsonArray messages = messagesFut.result().body();
                        JsonArray logicKeys = logicKeyFut.result().body();
                        JsonArray messagePara = new JsonArray();
                        for (int i = 0; i < messages.size(); i++) {
                            JsonObject jo = messages.getJsonObject(i);
                            messagePara.add(new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                                    .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, jo.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT)));
                        }

                        JsonArray logicKeyPara = new JsonArray();
                        for (int i = 0; i < logicKeys.size(); i++) {
                            JsonObject jo = logicKeys.getJsonObject(i);
                            logicKeyPara.add(new JsonObject().put("logic_key_id", jo.getInteger(ConstDef.LOGIC_KEY_DATA_KEYS.ID))
                                    .put("logic_key", jo.getString(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT)));
                        }

                        JsonObject malPara = new JsonObject().put("messages", messagePara).put("all_logic_Keys", logicKeyPara);
                        Promise<JsonObject> promise = Promise.promise();
                        JsonObject reqPara = new JsonObject();
                        reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.handle_knowledge_accumulation)
                                .put(EventConst.ALGO_API.KEYS.PARA, malPara);

                        context.vertx().eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                                .onFailure(promise::fail)
                                .onSuccess(v1 -> promise.complete(v1.body()));

                        promise.future()
                                .onFailure(failure -> log.error("handle_knowledge_accumulation failure: {}, {}", chatUuid, failure.getMessage()))
                                .onSuccess(v -> log.info("handle_knowledge_accumulation succeeded: {}", chatUuid));
                    }
                });

                // 3.更新对话回复策略
                messagesFut.compose(v -> {
                            JsonArray messages = v.body();
                            JsonArray messagePara = new JsonArray();
                            for (int i = 0; i < messages.size(); i++) {
                                JsonObject jo = messages.getJsonObject(i);
                                messagePara.add(new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                                        .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, jo.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT)));
                            }
                            Promise<JsonObject> promise = Promise.promise();
                            JsonObject malPara = new JsonObject().put("parent_name", userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME))
                                    .put("chat_uuid", chatUuid)
                                    .put("messages", messagePara);

                            JsonObject reqPara = new JsonObject();
                            reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_chat_reply_basis)
                                    .put(EventConst.ALGO_API.KEYS.PARA, malPara);

                            context.vertx().eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                                    .onFailure(promise::fail)
                                    .onSuccess(v1 -> promise.complete(v1.body()));

                            return promise.future();
                        }).compose(v -> {
                            v.put(ConstDef.REPLY_BASIS_DATA_KEYS.SUPPLYING, "CHAT").put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, chatUuid);
                            DeliveryOptions d8 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.ADD_REPLY_BASIS);
                            return context.vertx().eventBus().<JsonObject>request(EventConst.COMMON.ID, v, d8);
                        }).onSuccess(v -> log.info("update chat reply basis succeeded: {}", chatUuid))
                        .onFailure(failure -> log.error("update chat reply basis failure: {}, {}", chatUuid, failure.getMessage()));

                // 4. 更新家长回复策略
                chatsFut.compose(v -> {
                    JsonArray chats = v.body();
                    JsonArray chatUuids = new JsonArray();
                    Map<String, String> chatUuidLastestMsgTimeMap = new HashMap<>();
                    for (int i = 0; i < chats.size(); i++) {
                        JsonObject jo = chats.getJsonObject(i);
                        String uuid = jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                        chatUuids.add(uuid);
                        chatUuidLastestMsgTimeMap.put(uuid, jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME));
                    }
                    Promise<JsonArray> promise = Promise.promise();
                    DeliveryOptions d9 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_REPLY_BASIS_BY_OWNER_IDS);
                    JsonObject para9 = new JsonObject().put("ownerIds", chatUuids);
                    context.vertx().eventBus().<JsonArray>request(EventConst.COMMON.ID, para9, d9).onSuccess(data -> {
                        JsonArray chatsReplyBasis = data.body();
                        for (int i = 0; i < chatsReplyBasis.size(); i++) {
                            JsonObject jo = chatsReplyBasis.getJsonObject(i);
                            jo.remove(ConstDef.REPLY_BASIS_DATA_KEYS.ID);
                            jo.remove(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID);
                            jo.remove(ConstDef.REPLY_BASIS_DATA_KEYS.SUPPLYING);
                            jo.put("latest_message_time", chatUuidLastestMsgTimeMap.get(jo.getString(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID)));
                        }
                        promise.complete(chatsReplyBasis);
                    }).onFailure(failure -> {
                        promise.fail(failure.getMessage());
                    });

                    return promise.future();
                }).compose(chatReplyBasis -> {
                    Promise<JsonObject> mapParaPromise = Promise.promise();
                    messagesFut.onSuccess(v1 -> {
                        JsonArray messages = v1.body();
                        JsonArray messagePara = new JsonArray();
                        for (int i = 0; i < messages.size(); i++) {
                            JsonObject jo = messages.getJsonObject(i);
                            messagePara.add(new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                                    .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, jo.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT)));
                        }
                        JsonObject malPara = new JsonObject().put("messages", messagePara)
                                .put("current_chats_reply_basis", chatReplyBasis)
                                .put("parent_name", userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME));
                        mapParaPromise.complete(malPara);
                    }).onFailure(failure -> {
                        mapParaPromise.fail(failure.getMessage());
                    });

                    return mapParaPromise.future();
                }).compose(v -> {
                    Promise<JsonObject> promise = Promise.promise();

                    JsonObject reqPara = new JsonObject();
                    reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_parent_reply_basis)
                            .put(EventConst.ALGO_API.KEYS.PARA, v);

                    context.vertx().eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                            .onFailure(promise::fail)
                            .onSuccess(v1 -> promise.complete(v1.body()));

                    return promise.future();
                }).compose(v -> {
                    String parentTag = v.getString("tag");
                    v.remove("tag");
                    List<Future<Message<JsonObject>>> futures = new ArrayList<>();
                    if (!StringUtils.isNullOrEmpty(parentTag)) {
                        JsonObject updateParentTagPara = new JsonObject();
                        updateParentTagPara.put(ConstDef.USER_BASIS_DATA_KEYS.ID, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID))
                                .put(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY, userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY))
                                .put("tag", parentTag);
                        DeliveryOptions d8 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.UPDATE);
                        Future<Message<JsonObject>> fut1 = context.vertx().eventBus().request(EventConst.USER.ID, updateParentTagPara, d8);
                        futures.add(fut1);
                    }
                    v.put(ConstDef.REPLY_BASIS_DATA_KEYS.SUPPLYING, "PARENT").put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));
                    DeliveryOptions d8 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.ADD_REPLY_BASIS);
                    Future<Message<JsonObject>> fut2 = context.vertx().eventBus().request(EventConst.COMMON.ID, v, d8);
                    futures.add(fut2);

                    return Future.join(futures);
                }).onFailure(failure -> {
                    log.error("update parent reply basis failure: {}, {}", chatUuid, failure.getMessage());
                }).onSuccess(v -> {
                    log.info("update chat reply basis succeeded: {}", chatUuid);
                });
            }

        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void deleteMsg(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        JsonObject reqBody = context.body().asJsonObject();

        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.DELETE);
        context.vertx().eventBus().<Integer>request(EventConst.MESSAGE.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void updateChat(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        log.trace("[updateChat] userInfo is: {}", userInfo.encodePrettily());
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.UPDATE);
        context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());

            //生成提示消息
            if (reqBody.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_STATUS) && reqBody.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_STATUS) == ConstDef.CHAT_STATUS.EXPERT_ALREADY_INTERVENED) {
                JsonObject body = success.body();
                String receiverId = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);
                String senderId = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
                Integer receiverIdentity = body.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY);
                Integer senderIdentity = body.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                JsonObject para1 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, receiverId);
                context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para1, d2).compose(v1 -> {
                    Promise<Pair<String, String>> promise = Promise.promise();
                    String expertName = v1.body().getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME);
                    DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                    JsonObject para3 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, senderId);
                    context.vertx().eventBus().<JsonObject>request(EventConst.USER.ID, para3, d3).onComplete(done -> {
                        if (done.succeeded()) {
                            String parentName = done.result().body().getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME);
                            promise.complete(Pair.of(parentName, expertName));
                        } else {
                            promise.complete(Pair.of(null, expertName));
                        }
                    });

                    return promise.future();
                }).compose(names -> {
                    JsonArray replyMsgArray = new JsonArray();

                    JsonObject message = new JsonObject()
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, UUID.randomUUID() + "-msg")
                            .put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID))
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, ConstDef.USER_IDENTITY.BOT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID, "system")
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE, ConstDef.MSG_TYPE.SYSTEM)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CATEGORY, ConstDef.MSG_CATEGORY.TEXT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MACHINE_SCORE, null)
                            .put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.getTime());

                    JsonObject toExpertMsg = new JsonObject(message.encode()).put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, receiverId)
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, receiverIdentity)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, "你正在为" + names.getLeft() + "提供咨询服务");

                    JsonObject toParentMsg = new JsonObject(message.encode()).put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, senderId)
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, senderIdentity)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, "");

                    replyMsgArray.add(toExpertMsg).add(toParentMsg);

                    Promise<JsonArray> p = Promise.promise();
                    context.vertx().eventBus().<JsonObject>request(EventConst.MESSAGE.ID, new JsonObject().put("msgList", replyMsgArray), new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.ADD_BATCH))
                            .onSuccess(v -> p.complete(replyMsgArray))
                            .onFailure(failure -> p.fail(failure.getMessage()));

                    return p.future();
                }).onSuccess(v -> {
                    log.trace("chat status change, recommend msg: {}", v.encodePrettily());
                    WebClient client = WebClient.create(context.vertx());
                    PushUtils.pushAddedMsg(context.vertx(), client, redis, v, false);
                }).onFailure(failure -> {
                    log.error("generate reminder msg failure: {}", failure.getMessage());
                });
            }

            //更新状态和标题需要推送chat
            if (reqBody.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_STATUS) || reqBody.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE)) {
                JsonObject chat = success.body();
                String senderId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
                String receiverId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);

                int pusherIdentity;
                String pusherId;

                if (!receiverId.equals(userUid)) {
                    pusherIdentity = chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY);
                    pusherId = receiverId;
                } else {
                    pusherIdentity = chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY);
                    pusherId = senderId;
                }

                PushUtils.pushChatInfo(webClient, redis, pusherId, pusherIdentity, reqBody);
            }
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void deleteChatByUuid(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.DELETE);
        context.vertx().eventBus().<Integer>request(EventConst.CHAT.ID, reqBody, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void updateMessageFromWs(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject body = context.body().asJsonObject();
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.UPDATE);
        context.vertx().eventBus().<JsonObject>request(EventConst.MESSAGE.ID, body, d1).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", null).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getMessageFromWs(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);
        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        HttpServerRequest request = context.request();
        String messageUuid = request.getParam("message_uuid");

        JsonObject para1 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, messageUuid).put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);

        List<Future<Message<JsonObject>>> msgInfoFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> msgResourceFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> userBasisFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> expertInfoFutures = new ArrayList<>();
        Map<Integer, JsonObject> userMap = new HashMap<>();

        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ONE);
        context.vertx().eventBus().<JsonArray>request(EventConst.MESSAGE.ID, para1, d1).compose(v1 -> {
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p1 = Promise.promise();
            JsonArray messages = v1.body();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                String msgUuid = msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_MSG_INFO_BY_MSG_UUID);
                JsonObject para2 = new JsonObject().put(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID, msgUuid);
                msgInfoFutures.add(context.vertx().eventBus().request(EventConst.MESSAGE.ID, para2, d2));
            }

            Future.join(msgInfoFutures).onComplete(done -> p1.complete(Pair.of(messages, msgInfoFutures)));

            return p1.future();

        }).compose(v2 -> {
            Promise<Tuple> promise = Promise.promise();
            JsonArray left = v2.getLeft();
            JsonObject msg = left.getJsonObject(0);
            String chatUuid = msg.getString(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID);
            String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
            List<Future<Message<JsonObject>>> right = v2.getRight();
            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_BY_CHAT_UUID);
            JsonObject para2 = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, chatUuid);
            context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, para2, d2).onComplete(done -> {
                if (done.succeeded()) {
                    JsonObject body = done.result().body();
                    JsonArray deletedUsers = body.getJsonArray(ConstDef.CHAT_INFO_DATA_KEYS.DELETED_USERS);
                    if (deletedUsers != null && deletedUsers.contains(receiverId)) {
                        promise.complete(Tuple.of(left, right, false));
                    } else {
                        promise.complete(Tuple.of(left, right, true));
                    }
                } else {
                    promise.complete(Tuple.of(left, right, true));
                }
            });

            return promise.future();
        }).compose(v2 -> {
            JsonArray messages = v2.getJsonArray(0);
            Boolean receiverChatIsValid = v2.getBoolean(2);
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p2 = Promise.promise();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                Future<Message<JsonObject>> future = msgInfoFutures.get(i);
                if (future.succeeded()) {
                    JsonObject body = future.result().body();
                    body.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.ID);
                    msg.mergeIn(body);
                    msg.put("receiver_chat_valid", receiverChatIsValid);
                }

                Integer resourceId = msg.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.CHAT_RESOURCE_ID);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_RESOURCE_BY_ID);
                JsonObject para2 = new JsonObject().put(ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.ID, resourceId);
                msgResourceFutures.add(context.vertx().eventBus().request(EventConst.CHAT.ID, para2, d2));
            }

            Future.join(msgResourceFutures).onComplete(done -> p2.complete(Pair.of(messages, msgResourceFutures)));

            return p2.future();

        }).compose(v3 -> {
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p3 = Promise.promise();
            Set<String> userUids = new HashSet<>();
            JsonArray messages = v3.getLeft();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                Future<Message<JsonObject>> future = msgResourceFutures.get(i);
                if (future.succeeded())
                    msg.mergeIn(new JsonObject().put("resource_info", future.result().body()));
                Integer senderIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY);
                Integer receiverIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
                if (senderIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String senderId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
                    userUids.add(senderId);
                }
                if (receiverIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                    userUids.add(receiverId);
                }
            }
            for (String ele : userUids) {
                if ("system".equals(ele))
                    continue;
                DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                JsonObject para3 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, ele);
                userBasisFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para3, d3));
            }

            Future.join(userBasisFutures).onComplete(done -> p3.complete(Pair.of(messages, userBasisFutures)));

            return p3.future();

        }).compose(success -> {
            JsonArray messages = success.getLeft();
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p4 = Promise.promise();

            for (Future<Message<JsonObject>> future : userBasisFutures) {
                if (future.succeeded()) {
                    JsonObject userBasis = future.result().body();
                    Integer userId = userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                    userMap.put(userId, userBasis);
                    DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
                    JsonObject para4 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                    expertInfoFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para4, d4));
                }
            }

            Future.join(expertInfoFutures).onComplete(done -> p4.complete(Pair.of(messages, expertInfoFutures)));

            return p4.future();

        }).onSuccess(success -> {
            JsonArray messages = success.getLeft();
            Map<String, JsonObject> expertIdFieldMap = new HashMap<>();
            for (Future<Message<JsonObject>> future : expertInfoFutures) {
                if (future.succeeded()) {
                    JsonObject expertInfo = future.result().body();
                    Integer userId = expertInfo.getInteger(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID);
                    expertInfo.remove(ConstDef.EXPERT_INFO_DATA_KEYS.ID);
                    expertInfo.remove(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID);
                    expertInfo.remove(ConstDef.EXPERT_INFO_DATA_KEYS.PASSWORD);
                    expertInfo.put("expert_name", userMap.get(userId).getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME));
                    expertIdFieldMap.put(userMap.get(userId).getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID), expertInfo);
                }
            }

            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                Integer senderIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY);
                Integer receiverIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
                if (senderIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String senderId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
                    JsonObject expertInfo = expertIdFieldMap.get(senderId);
                    msg.mergeIn(new JsonObject().put("expert_info", expertInfo));
                }
                if (receiverIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                    JsonObject expertInfo = expertIdFieldMap.get(receiverId);
                    msg.mergeIn(new JsonObject().put("expert_info", expertInfo));
                }
            }

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", messages).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getMessages(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        HttpServerRequest request = context.request();
        String chatUuid = request.getParam("chat_uuid");
        String startTime = request.getParam("start_time");

        JsonObject para1 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid).put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
        if (!StringUtils.isNullOrEmpty(startTime)) {
            String time = TimeUtils.getTime(Long.parseLong(startTime));
            para1.put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, time);
        }

        List<Future<Message<JsonObject>>> msgInfoFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> msgResourceFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> userBasisFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> expertInfoFutures = new ArrayList<>();
        Map<Integer, JsonObject> userMap = new HashMap<>();

        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID);
        context.vertx().eventBus().<JsonArray>request(EventConst.MESSAGE.ID, para1, d1).compose(v1 -> {
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p1 = Promise.promise();
            JsonArray messages = v1.body();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                String msgUuid = msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_MSG_INFO_BY_MSG_UUID);
                JsonObject para2 = new JsonObject().put(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID, msgUuid);
                msgInfoFutures.add(context.vertx().eventBus().request(EventConst.MESSAGE.ID, para2, d2));
            }

            Future.join(msgInfoFutures).onComplete(done -> p1.complete(Pair.of(messages, msgInfoFutures)));

            return p1.future();

        }).compose(v2 -> {
            JsonArray messages = v2.getLeft();
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p2 = Promise.promise();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                Future<Message<JsonObject>> future = msgInfoFutures.get(i);
                if (future.succeeded()) {
                    JsonObject body = future.result().body();
                    body.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.ID);
                    msg.mergeIn(body);
                }
                Integer resourceId = msg.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.CHAT_RESOURCE_ID);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_RESOURCE_BY_ID);
                JsonObject para2 = new JsonObject().put(ConstDef.CHAT_RESOURCE.ID, resourceId);
                msgResourceFutures.add(context.vertx().eventBus().request(EventConst.CHAT.ID, para2, d2));
            }

            Future.join(msgResourceFutures).onComplete(done -> p2.complete(Pair.of(messages, msgResourceFutures)));

            return p2.future();

        }).compose(v3 -> {
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p3 = Promise.promise();
            Set<String> userUids = new HashSet<>();
            JsonArray messages = v3.getLeft();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                Future<Message<JsonObject>> future = msgResourceFutures.get(i);
                if (future.succeeded())
                    msg.mergeIn(new JsonObject().put("resource_info", future.result().body()));
                Integer senderIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY);
                Integer receiverIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
                if (senderIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String senderId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
                    userUids.add(senderId);
                }
                if (receiverIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                    userUids.add(receiverId);
                }
            }
            for (String ele : userUids) {
                if ("system".equals(ele))
                    continue;
                DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                JsonObject para3 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, ele);
                userBasisFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para3, d3));
            }

            Future.join(userBasisFutures).onComplete(done -> p3.complete(Pair.of(messages, userBasisFutures)));

            return p3.future();

        }).compose(success -> {
            JsonArray messages = success.getLeft();
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p4 = Promise.promise();

            for (Future<Message<JsonObject>> future : userBasisFutures) {
                if (future.succeeded()) {
                    JsonObject userBasis = future.result().body();
                    Integer userId = userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                    userMap.put(userId, userBasis);
                    DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
                    JsonObject para4 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                    expertInfoFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para4, d4));
                }
            }

            Future.join(expertInfoFutures).onComplete(done -> p4.complete(Pair.of(messages, expertInfoFutures)));

            return p4.future();

        }).onSuccess(success -> {
            JsonArray messages = success.getLeft();
            Map<String, JsonObject> expertIdFieldMap = new HashMap<>();
            for (Future<Message<JsonObject>> future : expertInfoFutures) {
                if (future.succeeded()) {
                    JsonObject expertInfo = future.result().body();
                    Integer userId = expertInfo.getInteger(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID);
                    expertInfo.remove(ConstDef.EXPERT_INFO_DATA_KEYS.ID);
                    expertInfo.remove(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID);
                    expertInfo.remove(ConstDef.EXPERT_INFO_DATA_KEYS.PASSWORD);
                    expertInfo.put("expert_name", userMap.get(userId).getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME));
                    expertIdFieldMap.put(userMap.get(userId).getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID), expertInfo);
                }
            }

            for (int i = 0; i < messages.size(); i++) {
                JsonObject msg = messages.getJsonObject(i);
                Integer senderIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY);
                Integer receiverIdentity = msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY);
                if (senderIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String senderId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
                    JsonObject expertInfo = expertIdFieldMap.get(senderId);
                    msg.mergeIn(new JsonObject().put("expert_info", expertInfo));
                }
                if (receiverIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                    String receiverId = msg.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                    JsonObject expertInfo = expertIdFieldMap.get(receiverId);
                    msg.mergeIn(new JsonObject().put("expert_info", expertInfo));
                }
            }
            JsonArray array = new JsonArray();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject jo = messages.getJsonObject(i);
                String senderId = jo.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID);
                String receiverId = jo.getString(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID);
                if (!userUid.equals(senderId) && !userUid.equals(receiverId))
                    continue;
                jo.put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME)));
                array.add(jo);
            }

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", array).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void createMessageFromWs(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        Vertx vertx = context.vertx();

        JsonObject userInfo = context.user().principal();
        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        log.trace("the createMsg userInfo: {}", userInfo.encodePrettily());
        JsonObject reqBody = context.body().asJsonObject();
        String chatUuid = reqBody.getString(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID);
        JsonObject queryChatPara = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, chatUuid);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_BY_CHAT_UUID);
        Promise<JsonObject> promise = Promise.promise();
        vertx.eventBus().<JsonObject>request(EventConst.CHAT.ID, queryChatPara, d1).onSuccess(v -> {
            JsonObject chatInfo = v.body();
            String senderId = chatInfo.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
            String receiverId = chatInfo.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);
            if (!userUid.equals(senderId)) {
                reqBody.put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, senderId);
                reqBody.put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, chatInfo.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY));
            }
            if (!userUid.equals(receiverId)) {
                reqBody.put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, receiverId);
                reqBody.put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, chatInfo.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY));
            }
            promise.complete(reqBody);
        }).onFailure(failure -> {
            promise.complete(reqBody);
        });

        promise.future().onComplete(done -> {
            JsonObject addPara = done.result();
            addPara.put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.getTime())
                    .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING);
            DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.ADD);
            vertx.eventBus().<JsonObject>request(EventConst.MESSAGE.ID, addPara, d).onSuccess(success -> {
                //创建消息并入库成功，可做后续操作
                JsonObject data = success.body();
                log.trace("the create msg res: {}", data.encodePrettily());
                JsonObject msg = data.getJsonObject("msg");
                JsonObject msgInfo = data.getJsonObject("msgInfo");
                JsonObject chatInfo = data.getJsonObject("chatInfo");

                //先响应给前端创建消息的结果
                JsonObject rt = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, msg.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID));
                response.setStatusCode(HttpStatus.SC_OK);
                response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", rt).encodePrettily());

                WebClient client = WebClient.create(context.vertx());
                if (chatInfo.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_STATUS) != ConstDef.CHAT_STATUS.EXPERT_ALREADY_INTERVENED &&
                        msg.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY) == ConstDef.USER_IDENTITY.PARENT) {

                    generateExpertReply(vertx, userInfo, chatInfo, msg).onComplete(ar -> {
                        if (ar.succeeded()) {
                            PushUtils.pushAddedMsg(vertx, client, redis, ar.result(), true);
                        } else {
                            log.error("Failed to generate expert reply", ar.cause());
                        }
                    });
                }

            }).onFailure(failure -> {
                response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
                response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
            });
        });
    }

    /**
     * 处理专家回复
     *
     * @param vertx
     * @param chat
     * @param msgBody
     * @return
     */
    private Future<JsonArray> generateExpertReply(Vertx vertx, JsonObject userInfo, JsonObject chat, JsonObject msgBody) {
        long start = System.currentTimeMillis();
        Promise<JsonArray> resPromise = Promise.promise();
        //get related paras
        Integer userId = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        String userUID = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        String chatUuid = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
        String chatTitle = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE);

        JsonObject queryParentPara = new JsonObject().put(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID, userId);
        DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_PARENT_INFO_BY_USER_ID);
        vertx.eventBus().<JsonObject>request(EventConst.USER.ID, queryParentPara, d).compose(v -> {
            JsonObject parentInfo = v.body();
            Integer parentId = parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);

            DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_CHILD_INFO_BY_PARENT_ID);
            JsonObject para2 = new JsonObject().put(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID, parentId);

            DeliveryOptions d3 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_FAMILY_INFO_BY_PARENT_ID);
            JsonObject para3 = new JsonObject().put(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID, parentId);

            DeliveryOptions d4 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID);
            JsonObject para4 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid).put("isFilterByDeleted", false);

            DeliveryOptions d5 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
            JsonObject para5 = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, userUID);

            DeliveryOptions d6 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.LOGIC_KEY.ACTIONS.QUERY_ALL);
            JsonObject para6 = new JsonObject();

            DeliveryOptions d7 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_ALL_EXPERTS);
            JsonObject para7 = new JsonObject();

            Promise<Tuple> promise = Promise.promise();

            Future<Message<JsonObject>> childFut = vertx.eventBus().request(EventConst.USER.ID, para2, d2);
            Future<Message<JsonObject>> familyFut = vertx.eventBus().request(EventConst.USER.ID, para3, d3);
            Future<Message<JsonArray>> messagesFut = vertx.eventBus().request(EventConst.MESSAGE.ID, para4, d4);
            Future<Message<JsonObject>> parentReplyBasisFut = vertx.eventBus().request(EventConst.COMMON.ID, para5, d5);
            Future<Message<JsonArray>> logicKeyFut = vertx.eventBus().request(EventConst.COMMON.ID, para6, d6);
            Future<Message<JsonArray>> allExpertsFut = vertx.eventBus().request(EventConst.USER.ID, para7, d7);

            Future.join(childFut, familyFut, messagesFut, parentReplyBasisFut, logicKeyFut, allExpertsFut).onComplete(done -> {
                if (done.succeeded()) {
                    JsonObject childInfo = childFut.result().body();
                    JsonObject familyInfo = familyFut.result().body();
                    JsonArray messages = messagesFut.result().body();
                    JsonObject replyBasis = parentReplyBasisFut.result().body();
                    JsonArray logicKeys = logicKeyFut.result().body();
                    JsonArray allExperts = allExpertsFut.result().body();
                    promise.complete(Tuple.of(parentInfo, childInfo, familyInfo, messages, replyBasis, logicKeys, allExperts));
                } else {
                    promise.fail(done.cause().getMessage());
                }
            });

            return promise.future();
        }).onSuccess(success -> {
            JsonObject parentInfo = success.getJsonObject(0);
            JsonObject childInfo = success.getJsonObject(1);
            JsonObject familyInfo = success.getJsonObject(2);
            JsonArray messages = success.getJsonArray(3);
            JsonObject replyBasis = success.getJsonObject(4);
            JsonArray logicKeys = success.getJsonArray(5);
            JsonArray experts = success.getJsonArray(6);

            JsonArray messagePara = new JsonArray();
            for (int i = 0; i < messages.size(); i++) {
                JsonObject jo = messages.getJsonObject(i);
                messagePara.add(new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, jo.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                        .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, jo.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT)));
            }

            JsonArray logicKeyPara = new JsonArray();
            for (int i = 0; i < logicKeys.size(); i++) {
                JsonObject jo = logicKeys.getJsonObject(i);
                logicKeyPara.add(new JsonObject().put("logic_key_id", jo.getInteger(ConstDef.LOGIC_KEY_DATA_KEYS.ID))
                        .put("logic_key", jo.getString(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT)));
            }

            JsonArray children = childInfo.getJsonArray("data");
            for (int i = 0; i < children.size(); i++) {
                JsonObject jo = children.getJsonObject(i);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.ID);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID);
                jo.remove(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID);
            }

            parentInfo.mergeIn(replyBasis);
            parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.ID);
            parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID);
            parentInfo.remove(ConstDef.PARENT_INFO_DATA_KEYS.PASSWORD);

            familyInfo.remove(ConstDef.FAMILY_INFO_DATA_KEYS.ID);
            familyInfo.remove(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID);

            WebClient client = WebClient.create(vertx);

            JsonObject requestBody = new JsonObject()
                    .put("current_content", msgBody.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT))
                    .put("messages", messagePara)
                    .put("parent_info", parentInfo)
                    .put("children_info", children)
                    .put("family_info", familyInfo)
                    .put("knowledge_base_uuid", chatUuid + "_" + userUID)
                    .put("all_logic_keys", logicKeyPara);

            Promise<JsonObject> malResPromise = Promise.promise();

            JsonObject reqPara = new JsonObject();
            reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_ai_reply)
                    .put(EventConst.ALGO_API.KEYS.PARA, requestBody);

            log.trace("generateExpertReply get para cost: {}, {}", System.currentTimeMillis() - start, reqPara);

            vertx.eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                    .onFailure(malResPromise::fail)
                    .onSuccess(v1 -> malResPromise.complete(v1.body()));

            malResPromise.future().compose(rawReply -> {
                Promise<Pair<JsonObject, String>> curExpertIdPromise = Promise.promise();
                if (chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID).equals("system")) {
                    Promise<JsonObject> recommendExpertPromise = Promise.promise();
                    JsonObject reqPara2 = new JsonObject();
                    reqPara2.put("messages", messagePara)
                            .put("experts", experts);

                    JsonObject para = new JsonObject();
                    para.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_recommend_expert_uuid)
                            .put(EventConst.ALGO_API.KEYS.PARA, reqPara2);

                    log.trace("the getRecommendUuid para: {}", para.encodePrettily());
                    vertx.eventBus().<JsonObject>request(EventConst.ALGO_API.ID, para, new DeliveryOptions().setSendTimeout(60 * 1000))
                            .onFailure(recommendExpertPromise::fail)
                            .onSuccess(v1 -> recommendExpertPromise.complete(v1.body()));

                    recommendExpertPromise.future().onComplete(done -> {
                        if (done.succeeded()) {
                            curExpertIdPromise.complete(Pair.of(rawReply, done.result().getString("expert_uuid")));
                        } else {
                            curExpertIdPromise.fail(done.cause().getMessage());
                        }
                    });
                } else {
                    String expertId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);
                    curExpertIdPromise.complete(Pair.of(rawReply, expertId));
                }

                return curExpertIdPromise.future();
            }).compose(rawReplyAndExpertId -> {
                JsonObject rawReply = rawReplyAndExpertId.getLeft();
                String recommendExpertId = rawReplyAndExpertId.getRight();
                String expertId = StringUtils.isNullOrEmpty(recommendExpertId) ? "system" : recommendExpertId;

                String allReply = rawReply.getString("all_reply");
                String newChatTitle = rawReply.getString("chat_title");
                JsonArray knowledgeUuids = rawReply.getJsonArray("knowledge_uuids");

                int messageType = ConstDef.MSG_TYPE.TEXT;
//                float machineScore = rawReply.getFloat("score_match", 0.0f);
//                int chatStatus = chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_STATUS);
//
//                // 推荐专家真人
//                if (machineScore < 0.5 && chatStatus == ConstDef.CHAT_STATUS.AI) {
//                    messageType = ConstDef.MSG_TYPE.BUTTON;
//                }

                // 更新title
                if (!newChatTitle.isEmpty() && !"chat".equals(chatTitle)) {
                    JsonObject para = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, chatUuid).put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE, newChatTitle);
                    DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.UPDATE);
                    vertx.eventBus().<JsonObject>request(EventConst.CHAT.ID, para, d1)
                            .onFailure(failure -> log.error("update chat title failure: {}, {}", chatUuid, failure.getMessage()))
                            .onSuccess(v -> {
                                JsonObject body = v.body();
                                String senderId = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
                                Integer senderIdentity = body.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY);
                                JsonObject rt = new JsonObject().put("chat_uuid", body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID))
                                        .put("chat_title", body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE));
                                PushUtils.pushChatInfo(webClient, redis, senderId, senderIdentity, rt);
                            });
                }
                //存储chat knowledge
                if (knowledgeUuids != null && !knowledgeUuids.isEmpty()) {
                    JsonArray para = new JsonArray();
                    for (int i = 0; i < knowledgeUuids.size(); i++) {
                        String knowledgeUuid = knowledgeUuids.getString(i);
                        para.add(new JsonObject().put(ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.KNOWLEDGE_UUID, knowledgeUuid).put(ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.CHAT_UUID, chatUuid));
                    }
                    vertx.eventBus().<JsonObject>request(EventConst.COMMON.ID, new JsonObject().put("knowledgeList", para),
                                    new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.CHAT_USED_KNOWLEDGE.ACTIONS.ADD_BATCH))
                            .onFailure(failure -> log.error("update chat used knowledge failure: {}, {}", chatUuid, failure.getMessage()));
                }

                if (chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID).equals("system") && !expertId.equals("system")) {
                    //给专家推送此chat
                    PushUtils.updatePushChatAfterRecommendExpert(vertx, client, redis, userInfo, chatUuid, expertId);
                    //推送系统消息
                    JsonArray replyMsgArray = new JsonArray();
                    JsonObject message = new JsonObject()
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, UUID.randomUUID() + "-msg")
                            .put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID))
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, ConstDef.USER_IDENTITY.BOT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID, "system")
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE, ConstDef.MSG_TYPE.SYSTEM)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CATEGORY, ConstDef.MSG_CATEGORY.TEXT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MACHINE_SCORE, null)
                            .put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.getTime());

                    JsonObject toParentMsg = new JsonObject(message.encode()).put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID))
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY))
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, "AI专家为您提供咨询服务");

                    replyMsgArray.add(toParentMsg);

                    vertx.eventBus().<JsonObject>request(EventConst.MESSAGE.ID, new JsonObject().put("msgList", replyMsgArray), new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.ADD_BATCH))
                            .onSuccess(v -> PushUtils.pushAddedMsg(vertx, webClient, redis, replyMsgArray, false))
                            .onFailure(failure -> log.error("add tip msg failure: {}, {}", chatUuid, failure.getMessage()));
                }

                String[] rawReplyList = allReply.split("\n\n");
                JsonArray replyMsgArray = new JsonArray();

                for (String rawReplyListContent : rawReplyList) {
                    String msgUuid = UUID.randomUUID() + "-msg";
                    JsonObject message = new JsonObject()
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, msgUuid)
                            .put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid)
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, ConstDef.USER_IDENTITY.BOT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID, chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID))
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, msgBody.getString(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID))
                            .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, msgBody.getInteger(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY))
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE, messageType)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CATEGORY, ConstDef.MSG_CATEGORY.TEXT)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, rawReplyListContent)
                            .put(ConstDef.MESSAGE_DATA_KEYS.MACHINE_SCORE, null)
                            .put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, TimeUtils.getTime());

                    replyMsgArray.add(message);

                    //异步获取音频路径并更新message_info
                    JsonObject queryAudioPathPara = new JsonObject();
                    queryAudioPathPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_audio_path)
                            .put(EventConst.ALGO_API.KEYS.PARA, new JsonObject().put("text", rawReplyListContent));

                    vertx.eventBus().<JsonObject>request(EventConst.ALGO_API.ID, queryAudioPathPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                            .onFailure(failure -> log.error("get ai_reply audio path failure: {}, {}", msgUuid, failure.getMessage()))
                            .onSuccess(v -> {
                                String audioPath = v.body().getString("audio_path");
                                DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.UPDATE_MSG_INFO);
                                JsonObject updateAudioPara = new JsonObject().put("chat_uuid", chatUuid)
                                        .put("message_uuid", msgUuid)
                                        .put(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_PATH, audioPath);

                                vertx.eventBus().<JsonObject>request(EventConst.MESSAGE.ID, updateAudioPara, d1)
                                        .onFailure(failure -> log.error("update audio path failure: {}, {}", msgUuid, failure.getMessage()));
                            });
                }

                Promise<JsonArray> p = Promise.promise();
                vertx.eventBus().<JsonObject>request(EventConst.MESSAGE.ID, new JsonObject().put("msgList", replyMsgArray),
                                new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.ADD_BATCH))
                        .onSuccess(v -> p.complete(replyMsgArray))
                        .onFailure(failure -> p.fail(failure.getMessage()));

                return p.future();
            }).onSuccess(resPromise::complete).onFailure(failure -> resPromise.fail(failure.getMessage()));

        }).onFailure(failure -> {
            resPromise.fail(failure.getMessage());
        });

        return resPromise.future();
    }


    private void getChats(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();

        String userUid = userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        JsonObject para = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, userUid);
        String createTime = context.request().getParam("create_time");
        if (!StringUtils.isNullOrEmpty(createTime)) {
            String time = TimeUtils.getTime(Long.parseLong(createTime));
            para.put(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME, time);
        }

        List<Future<Message<JsonObject>>> userBasisFutures = new ArrayList<>();
        List<Future<Message<JsonObject>>> expertInfoFutures = new ArrayList<>();
        Map<String, JsonObject> userMap = new HashMap<>();
        Map<Integer, String> userIdMap = new HashMap<>();

        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_ALL);
        context.vertx().eventBus().<JsonArray>request(EventConst.CHAT.ID, para, d1).compose(v1 -> {
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p = Promise.promise();
            Set<String> userUids = new HashSet<>();
            JsonArray chats = v1.body();
            log.trace("the chats is : {}", chats.encodePrettily());
            for (int i = 0; i < chats.size(); i++) {
                JsonObject chat = chats.getJsonObject(i);
                String senderId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
                String receiverId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);
                userUids.add(senderId);
                userUids.add(receiverId);
            }
            for (String ele : userUids) {
                if ("system".equals(ele))
                    continue;
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_BY_USER_UID);
                JsonObject para1 = new JsonObject().put(ConstDef.USER_BASIS_DATA_KEYS.USER_UID, ele);
                userBasisFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para1, d2));
            }

            Future.join(userBasisFutures).onComplete(done -> p.complete(Pair.of(chats, userBasisFutures)));

            return p.future();
        }).compose(v2 -> {
            JsonArray chats = v2.getLeft();
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> p = Promise.promise();
            for (Future<Message<JsonObject>> future : userBasisFutures) {
                if (future.succeeded()) {
                    JsonObject userBasis = future.result().body();
                    userMap.put(userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID), userBasis);
                    Integer userId = userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                    Integer userIdentity = userBasis.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
                    if (userIdentity == ConstDef.USER_IDENTITY.EXPERT) {
                        userIdMap.put(userId, userBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));
                        DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID);
                        JsonObject para2 = new JsonObject().put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                        expertInfoFutures.add(context.vertx().eventBus().request(EventConst.USER.ID, para2, d2));
                    }
                }
            }
            for (int i = 0; i < chats.size(); i++) {
                JsonObject chat = chats.getJsonObject(i);
                JsonObject senderBasis = userMap.getOrDefault(chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID), new JsonObject());
                JsonObject receiverBasis = userMap.getOrDefault(chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID), new JsonObject());
                chat.put("sender_name", senderBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME))
                        .put("sender_avatar", senderBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR))
                        .put("receiver_name", receiverBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME, "Ai小助手"))
                        .put("receiver_avatar", receiverBasis.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR));
            }

            Future.join(expertInfoFutures).onComplete(done -> p.complete(Pair.of(chats, expertInfoFutures)));

            return p.future();
        }).compose(success -> {
            List<Future<Message<JsonObject>>> chatRoundsFutures = new ArrayList<>();
            Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
            JsonArray chats = success.getLeft();
            Map<String, String> expertIdFieldMap = new HashMap<>();
            for (Future<Message<JsonObject>> future : expertInfoFutures) {
                if (future.succeeded()) {
                    JsonObject expertInfo = future.result().body();
                    Integer userId = expertInfo.getInteger(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID);
                    String field = expertInfo.getString(ConstDef.EXPERT_INFO_DATA_KEYS.FIELD);
                    expertIdFieldMap.put(userIdMap.get(userId), field);
                }
            }
            for (int i = 0; i < chats.size(); i++) {
                JsonObject chat = chats.getJsonObject(i);
                int senderIdentity = chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY) != null ? chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY) : -1;
                int receiverIdentity = chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY) != null ? chat.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY) : -1;
                if (senderIdentity == ConstDef.USER_IDENTITY.EXPERT)
                    chat.put("expert_field", expertIdFieldMap.get(chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID)));
                if (receiverIdentity == ConstDef.USER_IDENTITY.EXPERT)
                    chat.put("expert_field", expertIdFieldMap.get(chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID)));
            }
            for (int i = 0; i < chats.size(); i++) {
                JsonObject jo = chats.getJsonObject(i);
                String chatUuid = jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_CHAT_ROUNDS);
                JsonObject para2 = new JsonObject().put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid);
                chatRoundsFutures.add(context.vertx().eventBus().request(EventConst.MESSAGE.ID, para2, d2));

                jo.put(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME, TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME)));
                jo.put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME, TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME)));
                jo.put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME, TimeUtils.dateFormatTimestamp(jo.getString(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME)));
            }
            Future.join(chatRoundsFutures).onComplete(done -> promise.complete(Pair.of(chats, chatRoundsFutures)));

            return promise.future();
        }).onSuccess(success -> {
            JsonArray chats = success.getLeft();
            List<Future<Message<JsonObject>>> futures = success.getRight();
            for (int i = 0; i < futures.size(); i++) {
                Future<Message<JsonObject>> future = futures.get(i);
                if (future.succeeded()) {
                    chats.getJsonObject(i).mergeIn(future.result().body());
                }
            }

            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", chats).encodePrettily());
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }


    private void getRandomQuestion(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        log.trace("the userInfo: {}", userInfo.encodePrettily());
        Integer userIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        if (userIdentity != ConstDef.USER_IDENTITY.PARENT) {
            response.setStatusCode(HttpStatus.SC_FORBIDDEN);
            response.end(ResponseUtils.response(HttpStatus.SC_FORBIDDEN, "用户身份错误", null).encodePrettily());
            return;
        }
        DeliveryOptions d = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID);
        JsonObject para = new JsonObject().put(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID, userInfo.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID));
        context.vertx().eventBus().<JsonObject>request(EventConst.COMMON.ID, para, d).compose(v2 -> {
            Promise<JsonObject> promise = Promise.promise();
            JsonObject replyBasis = v2.body();
            replyBasis.remove(ConstDef.REPLY_BASIS_DATA_KEYS.ID);
            replyBasis.remove(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID);
            replyBasis.remove(ConstDef.REPLY_BASIS_DATA_KEYS.SUPPLYING);
            JsonObject reqPara = new JsonObject();
            reqPara.put(EventConst.ALGO_API.KEYS.URL, SysConfigPara.conf.chat_mal_api.get_random_questions)
                    .put(EventConst.ALGO_API.KEYS.PARA, replyBasis);

            context.vertx().eventBus().<JsonObject>request(EventConst.ALGO_API.ID, reqPara, new DeliveryOptions().setSendTimeout(60 * 1000))
                    .onFailure(promise::fail)
                    .onSuccess(v1 -> promise.complete(v1.body()));

            return promise.future();
        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, failure.getMessage(), null).encodePrettily());
        }).onSuccess(success -> {
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", success.getJsonArray("random_questions")).encodePrettily());
        });
    }


    private void createChat(RoutingContext context) {
        HttpServerResponse response = context.response();
        HttpUtils.setHttpHeader(response);
        HttpUtils.exceptionHandler(context.vertx(), response);

        JsonObject userInfo = context.user().principal();
        Integer senderIdentity = userInfo.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
        JsonObject reqBody = context.body().asJsonObject();
        reqBody.put(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY, senderIdentity)
                .put(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_IDENTITY, ConstDef.USER_IDENTITY.BOT);

        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.ADD);
        context.vertx().eventBus().<JsonObject>request(EventConst.CHAT.ID, reqBody, d1).onSuccess(success -> {
            JsonObject body = success.body();
            body.put("receiver_name", "Ai小助手");
            body.put(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME, TimeUtils.dateFormatTimestamp(body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME)));
            body.put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME, TimeUtils.dateFormatTimestamp(body.getString(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME)));
            body.put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME, TimeUtils.dateFormatTimestamp(body.getString(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME)));
            response.setStatusCode(HttpStatus.SC_OK);
            response.end(ResponseUtils.response(HttpStatus.SC_OK, "success", success.body()).encodePrettily());

        }).onFailure(failure -> {
            response.setStatusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
            response.end(ResponseUtils.response(HttpStatus.SC_INTERNAL_SERVER_ERROR, failure.getMessage(), null).encodePrettily());
        });
    }
}


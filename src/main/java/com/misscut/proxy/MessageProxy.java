package com.misscut.proxy;

import com.futureinteraction.utils.RedisHolder;
import com.misscut.event.ErrorCodes;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.proxy.dao.ChatDao;
import com.misscut.proxy.dao.MessageDao;
import com.misscut.utils.CommonUtils;
import com.misscut.utils.TimeUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class MessageProxy {
    private final Pool mySQLClient;
    private RedisHolder redis;
    private final MessageDao messageDao;
    private final ChatDao chatDao;
    private WebClient webClient;
    private Vertx vertx;

    public MessageProxy(Pool mySQLClient, RedisHolder redis, WebClient webClient, Vertx vertx) {
        this.mySQLClient = mySQLClient;
        this.redis = redis;
        this.messageDao = new MessageDao(mySQLClient);
        this.chatDao = new ChatDao(mySQLClient);
        this.webClient = webClient;
        this.vertx = vertx;
    }

    public void proc(Message<JsonObject> msg) {
        String action = msg.headers().get(BasicEventProcProxy.ACTION);

        switch (action) {
            case (EventConst.MESSAGE.ACTIONS.ADD) -> add(msg);
            case (EventConst.MESSAGE.ACTIONS.ADD_BATCH) -> addBatch(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_CHAT_UUID) -> queryAllByChatUuid(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_ALL_BY_PARA) -> queryAllByPara(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_ONE) -> queryOne(msg);
            case (EventConst.MESSAGE.ACTIONS.UPDATE) -> updateOne(msg);
            case (EventConst.MESSAGE.ACTIONS.UPDATE_MSG_INFO) -> updateMsgInfoOne(msg);
            case (EventConst.MESSAGE.ACTIONS.DELETE) -> deleteOne(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_MSG_INFO_BY_MSG_UUID) -> queryMsgInfoByMsgUuid(msg);
//            case (EventConst.MESSAGE.ACTIONS.QUERY_MSG_RESOURCE_BY_MSG_UUID) -> queryMsgResourceByMsgUuid(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_LATEST_MSG_BY_CHAT_UUID_AND_SENDER) -> queryLatestMsgByChatUuidAndSender(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_LATEST_MSG_BY_CHAT_UUID) -> queryLatestMsgByChatUuid(msg);
            case (EventConst.MESSAGE.ACTIONS.QUERY_CHAT_ROUNDS) -> queryChatRounds(msg);

            default -> msg.fail(ErrorCodes.INVALID_ACTION, "invalid action: " + action);
        }
    }


    private void queryChatRounds(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.getChatRounds(conn, chatUuid)
                                .compose(v3 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v3);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryChatRounds]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }



    private void queryLatestMsgByChatUuid(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString("chat_uuid");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.getLatestMsgByChatUuid(conn, chatUuid)
                                .compose(v3 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v3);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryLatestMsgByChatUuid]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }



    private void queryLatestMsgByChatUuidAndSender(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString("chat_uuid");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.getLatestMsgByChatUuidAndSender(conn, chatUuid, ConstDef.USER_IDENTITY.PARENT)
                                .compose(v1 -> {
                                   Promise<String> p1 = Promise.promise();
                                   p1.complete(v1.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT));

                                   return p1.future();
                                })
                                .compose(v2 -> {
                                    Promise<JsonObject> msgContentPromise = Promise.promise();
                                    JsonObject res = new JsonObject().put("parent_content", v2);
                                    messageDao.getLatestMsgByChatUuidAndSender(conn, chatUuid, ConstDef.USER_IDENTITY.EXPERT).onComplete(done -> {
                                        if (done.succeeded()) {
                                            res.put("expert_content", done.result().getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT));
                                        } else {
                                            res.put("expert_content", null);
                                        }
                                        msgContentPromise.complete(res);
                                    });

                                    return msgContentPromise.future();
                                })
                                .compose(v3 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v3);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryLatestMsgByChatUuidAndSender]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void updateMsgInfoOne(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString("chat_uuid");
        String messageUuid = body.getString("message_uuid");
        body.remove("chat_uuid");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.getMsgByChatUuidAndMsgUuid(conn, chatUuid, messageUuid)
                                .compose(v1 -> {
                                    if (v1.size() > 1)
                                        return Future.failedFuture("invalid para");
                                    return Future.succeededFuture(v1.getJsonObject(0));
                                })
                                .compose(v2 -> {
                                    Promise<JsonObject> promise = Promise.promise();
                                    JsonObject res = new JsonObject();
                                    res.put("msg", v2);
                                    messageDao.updateMsgInfoOne(conn, messageUuid, body).onComplete(done -> {
                                        if (done.succeeded()) {
                                            res.put("msgInfo", done.result());
                                        } else {
                                            res.put("msgInfo", null);
                                        }
                                        promise.complete(res);
                                    });

                                    return promise.future();
                                })
                                .compose(v4 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v4);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[updateMsgInfoOne]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }



    private void deleteOne(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString("chat_uuid");
        String messageUuid = body.getString("message_uuid");
        String userUid = body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        JsonObject row = new JsonObject().put("is_deleted", 1).put("deleted_at", TimeUtils.getTime());

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.getMsgByChatUuidAndMsgUuid(conn, chatUuid, messageUuid)
                                .compose(v1 -> {
                                    if (v1.size() > 1)
                                        return Future.failedFuture("invalid para");
                                    return Future.succeededFuture();
                                })
                                .compose(v1 -> messageDao.updateByMsgUuidDeleted(conn, userUid, messageUuid))
//                                .compose(v2 -> messageDao.updateMsgInfoOne(conn, messageUuid, row))
                                .compose(v4 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v4);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[deleteOne]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void updateOne(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String messageUuid = body.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.updateOne(conn, messageUuid, body)
                                .compose(v1 -> {
                                    String chatUuid = v1.getString(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID);
                                    JsonObject row = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME, TimeUtils.getTime());
                                    return chatDao.updateOne(conn, chatUuid, row);
                                })
                                .compose(v2 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v2);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[updateOne]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));

    }


//    private void queryMsgResourceByMsgUuid(Message<JsonObject> msg) {
//        String messageUuid = msg.body().getString(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID);
//
//        mySQLClient.getConnection().onSuccess(conn ->
//                conn.begin()
//                        .compose(tx -> messageDao.getMsgResourceByMsgUuid(conn, messageUuid)
//                                .compose(v1 -> {
//                                    tx.commit();
//                                    return Future.succeededFuture(v1);
//                                }))
//                        .eventually(v -> conn.close())
//                        .onFailure(failure -> {
//                            log.error("Transaction failed: {}", failure.getMessage());
//                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
//                        }).onSuccess(v -> {
//                            log.debug("Transaction succeeded");
//                            msg.reply(v);
//                        }));
//
//    }



    private void queryMsgInfoByMsgUuid(Message<JsonObject> msg) {
        String messageUuid = msg.body().getString(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.getMsgInfoByMsgUuid(conn, messageUuid)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryMsgInfoByMsgUuid]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));

    }


    private void queryAllByPara(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        List<String> queryParas = new ArrayList<>();

        for (String fieldName : body.fieldNames()) {
            queryParas = CommonUtils.updateParaList(queryParas, fieldName, body.getValue(fieldName), ":=:");
        }

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.queryAll(conn, "", finalQueryParas, false)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryAll]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v.getRight());
                        }));
    }


    private void queryAllByChatUuid(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        List<String> queryParas = new ArrayList<>();
        String userUid = body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        Boolean isFilterByDeleted = body.getBoolean("isFilterByDeleted", true);
        body.remove("isFilterByDeleted");
        body.remove(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        queryParas.add("[" + ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID + ":=:" + body.getString(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID) + "]");

        if (body.containsKey(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME))
            queryParas = CommonUtils.updateParaList(queryParas, ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, body.getString(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME), ":>:");

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.queryAll(conn, userUid, finalQueryParas, isFilterByDeleted)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryAll]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v.getRight());
                        }));
    }


    private void queryOne(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String userUid = body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        body.remove(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        List<String> queryParas = new ArrayList<>();
        for (String field : body.fieldNames()) {
            queryParas = CommonUtils.updateParaList(queryParas, field, body.getString(field), ":=:");
        }

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.queryAll(conn, userUid, finalQueryParas, true)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryOne]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v.getRight());
                        }));
    }


    private void addBatch(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        JsonArray msgList = body.getJsonArray("msgList");
        JsonArray msgListPara = new JsonArray();
        JsonArray msgInfoListPara = new JsonArray();

        String time = TimeUtils.getTime();
        String chatUuid = msgList.getJsonObject(0).getString(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID);

        for (int i = 0; i < msgList.size(); i++) {
            JsonObject data = msgList.getJsonObject(i);
            Pair<JsonObject, JsonObject> row = genMsgPara(data);
            msgListPara.add(row.getLeft().put(ConstDef.MESSAGE_DATA_KEYS.CREATE_TIME, time));
            msgInfoListPara.add(row.getRight());
        }

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.addMsgBatch(conn, msgListPara)
                                .compose(v -> messageDao.addMsgInfoBatch(conn, msgInfoListPara))
                                .compose(v -> {
                                    JsonObject row = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME, time);
                                    return chatDao.updateOne(conn, chatUuid, row);
                                })
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[addBatch]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void add(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        Pair<JsonObject, JsonObject> para = genMsgPara(body);
        JsonObject msgPara = para.getLeft();
        JsonObject msgInfoPara = para.getRight();

        String time = TimeUtils.getTime();

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> messageDao.addMsgOne(conn, msgPara)
                                .compose(msgData -> {
                                    Promise<Tuple> promise = Promise.promise();
                                    msgInfoPara.put(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID, msgData.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID));
                                    messageDao.addMsgInfoOne(conn, msgInfoPara).onComplete(done -> promise.complete(Tuple.of(msgData, done.result())));

                                    return promise.future();
                                })
                                .compose(v -> {
                                    Promise<JsonObject> p = Promise.promise();
                                    JsonObject row = new JsonObject().put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_MESSAGE_TIME, time);
                                    chatDao.updateOne(conn, body.getString("chat_uuid"), row).onComplete(done -> {
                                        JsonObject res = new JsonObject().put("msg", v.getJsonObject(0))
                                                .put("msgInfo", v.getJsonObject(1))
                                                .put("chatInfo", done.result());
                                        p.complete(res);
                                    });

                                    return p.future();
                                })
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[add]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }



    private Pair<JsonObject, JsonObject> genMsgPara(JsonObject data) {
        JsonObject msgInfoPara = new JsonObject()
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.ID, 0)
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID, data.getString("message_uuid"))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.QUOTE_CONTENT, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.QUOTE_CONTENT))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.QUOTE_MESSAGE_UUID, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.QUOTE_MESSAGE_UUID))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_PATH, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_PATH))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_DURATION, data.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_DURATION))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.CARD_TYPE, data.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.CARD_TYPE))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.CHAT_RESOURCE_ID, data.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.CHAT_RESOURCE_ID))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.BUTTON_TYPE, data.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.BUTTON_TYPE))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_SCORE, data.getFloat(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_SCORE))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_FEEDBACK, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_FEEDBACK))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_REVISION, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_REVISION))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_LIKE_STATUS, data.getInteger(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_LIKE_STATUS))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_SCORE, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_SCORE))
                .put(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_FEEDBACK, data.getString(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_FEEDBACK));

        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.ID);
//        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.QUOTE_CONTENT);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.QUOTE_MESSAGE_UUID);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_PATH);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.AUDIO_DURATION);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.CARD_TYPE);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.CHAT_RESOURCE_ID);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.BUTTON_TYPE);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_SCORE);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_FEEDBACK);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.EXPERT_REVISION);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_LIKE_STATUS);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_SCORE);
        data.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.PARENT_FEEDBACK);

        return Pair.of(data, msgInfoPara);
    }


}

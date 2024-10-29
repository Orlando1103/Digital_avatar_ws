package com.misscut.proxy;

import com.futureinteraction.utils.RedisHolder;
import com.misscut.event.ErrorCodes;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.proxy.dao.ChatDao;
import com.misscut.proxy.dao.MessageDao;
import com.misscut.proxy.dao.UserDao;
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
import lombok.extern.slf4j.Slf4j;

import java.sql.Time;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;


@Slf4j
public class ChatProxy {
    private final Pool mySQLClient;
    private RedisHolder redis;
    private final ChatDao chatDao;
    private final UserDao userDao;
    private final MessageDao messageDao;
    private WebClient webClient;
    private Vertx vertx;

    public ChatProxy(Pool mySQLClient, RedisHolder redis, WebClient webClient, Vertx vertx) {
        this.mySQLClient = mySQLClient;
        this.redis = redis;
        this.chatDao = new ChatDao(mySQLClient);
        this.userDao = new UserDao(mySQLClient);
        this.messageDao = new MessageDao(mySQLClient);
        this.webClient = webClient;
        this.vertx = vertx;
    }

    public void proc(Message<JsonObject> msg) {
        String action = msg.headers().get(BasicEventProcProxy.ACTION);

        switch (action) {
            case (EventConst.CHAT.ACTIONS.ADD) -> add(msg);
            case (EventConst.CHAT.ACTIONS.UPDATE) -> update(msg);
            case (EventConst.CHAT.ACTIONS.DELETE) -> deleteOne(msg);
            case (EventConst.CHAT.ACTIONS.QUERY_ALL) -> queryAll(msg);
            case (EventConst.CHAT.ACTIONS.QUERY_ALL_BY_PARA) -> queryAllByPara(msg);
            case (EventConst.CHAT.ACTIONS.QUERY_BY_CHAT_UUID) -> queryByUUId(msg);
            case (EventConst.CHAT.ACTIONS.QUERY_RESOURCE) -> queryResource(msg);
            case (EventConst.CHAT.ACTIONS.DELETE_RESOURCE) -> deleteResource(msg);
            case (EventConst.CHAT.ACTIONS.QUERY_RESOURCE_BY_ID) -> queryResourceById(msg);
            case (EventConst.CHAT.ACTIONS.QUERY_CHAT_BY_TIME_DIFF) -> queryChatByTimeDiff(msg);
            case (EventConst.CHAT.ACTIONS.ADD_BATCH_CHAT_RESOURCE) -> addBatchChatResource(msg);

            default -> msg.fail(ErrorCodes.INVALID_ACTION, "invalid action: " + action);
        }
    }


    private void queryAllByPara(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        List<String> queryParas = new ArrayList<>();
        Boolean isFilterByDeleted = body.getBoolean("isFilterByDeleted", true);
        body.remove("isFilterByDeleted");

        for (String fieldName : body.fieldNames()) {
            queryParas = CommonUtils.updateParaList(queryParas, fieldName, body.getValue(fieldName), ":=:");
        }

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.queryAll(conn, "", finalQueryParas, isFilterByDeleted)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> {
                            return conn.close().onFailure(closeFailure -> {
                                log.error("[queryAll]Connection close failed: {}", closeFailure.getMessage());
                            });
                        })
                        .onFailure(failure -> {
                            log.error("[queryAll]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v.getRight());
                        }));
    }




    private void addBatchChatResource(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        JsonArray resourceList = body.getJsonArray("resourceList");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.addChatResourceBatch(conn, resourceList)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .compose(chatDao::queryByIds)
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[addBatch]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryResourceById(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer id = body.getInteger(ConstDef.CHAT_RESOURCE.ID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.getResourceById(conn, id)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryResourceById]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }



    private void queryChatByTimeDiff(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer timeDiff = body.getInteger("time_diff");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.getChatsBtTimeDiff(conn, timeDiff)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryChatByTimeDiff]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }

    private void deleteResource(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString(ConstDef.CHAT_RESOURCE.CHAT_UUID);
        Integer id = body.getInteger(ConstDef.CHAT_RESOURCE.ID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.updateResourceByChatUuidAndResourceId(conn, chatUuid, id)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[deleteResource]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }



    private void queryResource(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString(ConstDef.CHAT_RESOURCE.CHAT_UUID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.getResourceByChatUuid(conn, chatUuid)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryResource]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryByUUId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.getChatInfoByChatUuid(conn, chatUuid)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryByUUId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void update(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
        String userUid = body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        body.remove(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        if (body.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME)) {
            String time = TimeUtils.getTime(body.getLong(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME));
            body.put(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME, time);
        }
        if (body.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_STATUS)) {
            if (body.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_STATUS) == ConstDef.CHAT_IS_FAVORITE.COLLECT)
                body.put(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_TIME, TimeUtils.getTime());
            else
                body.put(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_TIME, null);
        }

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.updateOne(conn, chatUuid, body)
                                .compose(v1 -> {
                                    if (body.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME)) {
                                        Promise<JsonObject> p = Promise.promise();
                                        String time = body.getString(ConstDef.CHAT_INFO_DATA_KEYS.LATEST_READ_TIME);
                                        messageDao.updateBatchMsgStatus(conn, chatUuid, userUid, time, ConstDef.MSG_STATUS.READ)
                                                .onComplete(done -> p.complete(v1));
                                        return p.future();
                                    } else {
                                        return Future.succeededFuture(v1);
                                    }
                                })
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[update]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void deleteOne(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString("chat_uuid");
        String userUid = body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);

        JsonObject row = new JsonObject().put("is_deleted", 1).put("deleted_at", TimeUtils.getTime());

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.getChatInfoByChatUuid(conn, chatUuid)
                                .compose(chat -> {
                                    if (chat == null)
                                        return Future.failedFuture("no such chat");
                                    String senderId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
                                    String receiverId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID);
                                    if (!userUid.equals(senderId) && !userUid.equals(receiverId))
                                        return Future.failedFuture("no right to access");

                                    return chatDao.updateChatOneDeleted(conn, userUid, chatUuid);
                                })
                                .compose(v1 -> chatDao.updateResourceByChatUuid(conn, chatUuid, row))
//                                .compose(v3 -> chatDao.updateUsedKnowledge(conn, chatUuid, row))
//                                .compose(v4 -> messageDao.getMsgByChatUuid(conn, chatUuid))
//                                .compose(v5 -> {
//                                    List<String> ids = v5.stream()
//                                            .map(o -> (JsonObject) o)
//                                            .map(x -> x.getString(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID))
//                                            .collect(Collectors.toList());
//
//                                    return messageDao.updateMsgInfoBatch(conn, row.getString("deleted_at"), new JsonArray(ids));
//                                })
                                .compose(v6 -> messageDao.updateByChatUuidDeleted(conn, userUid, chatUuid))
                                .compose(v7 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v7);
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


    private void queryAll(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String userUid = body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID);
        Boolean isFilterByDeleted = body.getBoolean("isFilterByDeleted", true);
        body.remove("isFilterByDeleted");
        List<String> queryParas = new ArrayList<>();
        queryParas.add("[" + ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID + ":=:" + userUid + "]");
        queryParas.add("[" + ConstDef.CHAT_INFO_DATA_KEYS.RECEIVER_ID + ":=:" + userUid + "]");

        if (body.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME))
            queryParas = CommonUtils.updateParaList(queryParas, ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME, body.getString(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME), ":>=:");

        if (body.containsKey(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_STATUS))
            queryParas = CommonUtils.updateParaList(queryParas, ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_STATUS, body.getInteger(ConstDef.CHAT_INFO_DATA_KEYS.FAVORITE_STATUS), ":=:");


        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> chatDao.queryAll(conn, userUid, finalQueryParas, isFilterByDeleted)
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


    private void add(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> {
                            body.put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, UUID.randomUUID() + "-ct")
                                    .put(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_TITLE, "chat")
                                    .put(ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME, TimeUtils.getTime());
                            return chatDao.addChatOne(conn, body)
                                    .compose(v1 -> {
                                        tx.commit();
                                        return Future.succeededFuture(v1);
                                    });
                        })
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[add]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }

}

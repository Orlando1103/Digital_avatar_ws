package com.misscut.proxy;

import com.futureinteraction.utils.RedisHolder;
import com.misscut.event.ErrorCodes;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.proxy.dao.CommonDao;
import com.misscut.utils.CommonUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Pool;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;


@Slf4j
public class CommonProxy {
    private final Pool mySQLClient;
    private RedisHolder redis;
    private final CommonDao commonDao;
    private WebClient webClient;
    private Vertx vertx;

    public CommonProxy(Pool mySQLClient, RedisHolder redis, WebClient webClient, Vertx vertx) {
        this.mySQLClient = mySQLClient;
        this.redis = redis;
        this.commonDao = new CommonDao(mySQLClient);
        this.webClient = webClient;
        this.vertx = vertx;
    }

    public void proc(Message<JsonObject> msg) {
        String action = msg.headers().get(BasicEventProcProxy.ACTION);

        switch (action) {
            case (EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_BY_OWNER_ID) -> queryByOwnerId(msg);
            case (EventConst.COMMON.REPLY_BASIS.ACTIONS.ADD_REPLY_BASIS) -> addReplyBasis(msg);
            case (EventConst.COMMON.LOGIC_KEY.ACTIONS.QUERY_ALL) -> queryAllLogicKey(msg);
            case (EventConst.COMMON.LOGIC_KEY.ACTIONS.ADD_LOGIC_KEY) -> addLogicKey(msg);
            case (EventConst.COMMON.LOGIC.ACTIONS.ADD_LOGIC) -> addLogic(msg);
            case (EventConst.COMMON.LOGIC.ACTIONS.QUERY_BY_LOGIC_KEY_ID) -> queryLogicByKeyId(msg);
            case (EventConst.COMMON.CHAT_USED_KNOWLEDGE.ACTIONS.ADD_BATCH) -> addBatch(msg);
            case (EventConst.COMMON.CHAT_USED_KNOWLEDGE.ACTIONS.QUERY_TOP_N_BY_CHAT_UUID) -> queryTopNByChatUuid(msg);
            case (EventConst.COMMON.REPLY_BASIS.ACTIONS.QUERY_REPLY_BASIS_BY_OWNER_IDS) -> queryReplyBasisByOwnerIds(msg);

            default -> msg.fail(ErrorCodes.INVALID_ACTION, "invalid action: " + action);
        }
    }


    private void queryReplyBasisByOwnerIds(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        JsonArray ownerIds = body.getJsonArray("ownerIds");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.getReplyBasisByIds(conn, ownerIds)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[addReplyBasis]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void addReplyBasis(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.addUpdateReplyBasis(conn, body)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[addReplyBasis]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryTopNByChatUuid(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String chatUuid = body.getString("chat_uuid");
        Integer topN = body.getInteger("topN");

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.getTopNChatUsedKnowledgeByChatUuid(conn, chatUuid, topN)
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


    private void queryLogicByKeyId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer keyId = body.getInteger(ConstDef.LOGIC_DATA_KEYS.LOGIC_KEY_ID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.getLogicByKeyId(conn, keyId)
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


    private void queryByOwnerId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String ownerId = body.getString(ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID);

        commonDao.getOneReplyBasisByOwnerId(ownerId)
                .onSuccess(msg::reply)
                .onFailure(fail -> msg.fail(ErrorCodes.DB_ERROR, fail.getMessage()));
    }


    private void addBatch(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        JsonArray knowledgeList = body.getJsonArray("knowledgeList");

        for (int i = 0; i < knowledgeList.size(); i++) {
            JsonObject data = knowledgeList.getJsonObject(i);
            data.put("frequency", 1);
        }

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.addUsedKnowledgeBatch(conn, knowledgeList)
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


    private void queryAllLogicKey(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        List<String> queryParas = new ArrayList<>();

        if (body.containsKey(ConstDef.LOGIC_KEY_DATA_KEYS.ID))
            queryParas = CommonUtils.updateParaList(queryParas, ConstDef.LOGIC_KEY_DATA_KEYS.ID, body.getInteger(ConstDef.LOGIC_KEY_DATA_KEYS.ID), ":=:");

        if (body.containsKey(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT))
            queryParas = CommonUtils.updateParaList(queryParas, ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT, body.getString(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT), ":=:");

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.queryAllLogicKey(conn, finalQueryParas)
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


    private void addLogicKey(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        JsonObject logicKeyRow = new JsonObject().put("key_text", body.getString(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT));
        body.remove(ConstDef.LOGIC_KEY_DATA_KEYS.KEY_TEXT);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.addLogicKeyOne(conn, logicKeyRow)
                                .compose(v -> {
                                    Integer logicKeyId = v.getInteger(ConstDef.LOGIC_KEY_DATA_KEYS.ID);
                                    Promise<JsonObject> p = Promise.promise();
                                    body.put(ConstDef.LOGIC_DATA_KEYS.LOGIC_KEY_ID, logicKeyId);
                                    commonDao.addLogicOne(conn, body).onComplete(done -> p.complete(done.result()));

                                    return p.future();
                                })
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
                            msg.reply(v);
                        }));
    }


    private void addLogic(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> commonDao.addLogicOne(conn, body)
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
                            msg.reply(v);
                        }));
    }
}

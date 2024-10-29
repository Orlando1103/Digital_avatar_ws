package com.misscut.proxy.dao;

import com.misscut.model.ConstDef;
import com.misscut.utils.mysql.AbstractDao;
import com.misscut.utils.mysql.NativeSqlUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Pool;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import io.vertx.sqlclient.Tuple;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;

/**
 * @author jiyifei
 * @date 2023/9/26 上午10:54
 */
@Slf4j
public class MessageDao extends AbstractDao {

    public final static String TABLE = "message";
    public final static String MESSAGE_INFO_TABLE = "message_info";


    public MessageDao(Pool mySQLClient) {
        super(mySQLClient);
    }
    private final String QUERY_SQL = "SELECT * FROM " + TABLE;
    private final String QUERY_COUNT_SQL = "SELECT count(*) FROM " + TABLE;
    private final String QUERY_BY_ID_SQL = QUERY_SQL + " WHERE " + ConstDef.MESSAGE_DATA_KEYS.ID + " = ?";
    private final String QUERY_BY_MSG_UUID_SQL = QUERY_SQL + " WHERE " + ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID + " = ?";
    private final String QUERY_BY_CHAT_UUID_SQL = QUERY_SQL + " WHERE " + ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID + " = ?";
    private final String INSERT_MSG_LIST_SQL = "INSERT INTO message (id, message_uuid, chat_uuid, message_type, message_category, message_content, sender_identity, " +
            "receiver_identity, sender_id, receiver_id, machine_score, message_status, create_time, is_deleted, deleted_at) VALUES ";




    private final String QUERY_MSG_INFO_SQL = "SELECT * FROM " + MESSAGE_INFO_TABLE;
    private final String QUERY_MSG_INFO_COUNT_SQL = "SELECT count(*) FROM " + MESSAGE_INFO_TABLE;
    private final String QUERY_MSG_INFO_BY_ID_SQL = QUERY_MSG_INFO_SQL + " WHERE " + ConstDef.MESSAGE_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_MSG_INFO_BY_MSG_UUID_ID_SQL = QUERY_MSG_INFO_SQL + " WHERE " + ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID + " = ?";
    private final String UPDATE_MSG_INFO_BATCH_SQL = "UPDATE " + MESSAGE_INFO_TABLE;
    private final String INSERT_MSG_INFO_LIST_SQL = "INSERT INTO message_info (id, message_uuid, quote_content, quote_message_uuid, audio_path, audio_duration, card_type, resource_id, " +
            "button_type, expert_score, expert_feedback, expert_revision, parent_like_status, parent_score, parent_feedback, is_deleted, deleted_at) VALUES ";



    public Future<Integer> updateBatchMsgStatus(SqlConnection conn, String chatUuid, String senderId, String time, String status) {
        String sql = "UPDATE message SET message_status = ? WHERE chat_uuid = ? AND sender_id != ? AND create_time <= ?";
        Tuple para = Tuple.of(status, chatUuid, senderId, time);

        Promise<Integer> promise = Promise.promise();
        commonBatchUpdate(conn, sql, para)
                .onSuccess(success -> promise.complete(success.rowCount()))
                .onFailure(failure -> promise.fail(failure.getMessage()));

        return promise.future();
    }


    public Future<JsonObject> updateOne(SqlConnection conn, String uuid, JsonObject row) {
        row.remove(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID);
        Pair<String, Tuple> pairs = genUpdateDataByPara(TABLE, ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(uuid);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_BY_MSG_UUID_SQL, uuid).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> updateMsgInfoOne(SqlConnection conn, String uuid, JsonObject row) {
        row.remove(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID);

        Pair<String, Tuple> pairs = genUpdateDataByPara(MESSAGE_INFO_TABLE, ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(uuid);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_MSG_INFO_BY_MSG_UUID_ID_SQL, uuid).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<Integer> updateByChatUuid(SqlConnection conn, String uuid, JsonObject row) {
        row.remove(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID);
        Pair<String, Tuple> pairs = genUpdateDataByPara(TABLE, ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(uuid);

        Promise<Integer> promise = Promise.promise();

        commonBatchUpdate(conn, updateSql, para)
                .onFailure(promise::fail)
                .onSuccess(success -> promise.complete(success.rowCount()));

        return promise.future();
    }


    public Future<JsonObject> updateByChatUuidDeleted(SqlConnection conn, String userUid, String uuid) {
//        String sql = "UPDATE message SET deleted_users = JSON_ARRAY_APPEND(COALESCE(deleted_users, '[]'), '$', '" + userUid +
//                "') WHERE chat_uuid = ? AND NOT JSON_CONTAINS(COALESCE(deleted_users, '[]'), '" + "\"" + userUid + "\"" + "', '$')";


        String sql = "UPDATE message " +
                "SET deleted_users = CASE " +
                "        WHEN NOT JSON_CONTAINS(COALESCE(deleted_users, '[]'), '" + "\"" + userUid + "\"" + "', '$')" +
                "        THEN JSON_ARRAY_APPEND(COALESCE(deleted_users, '[]'), '$', '" + userUid + "')" +
                "        ELSE deleted_users" +
                "    END " +
                "WHERE chat_uuid = ?";

        Tuple tuple = Tuple.tuple();
        tuple.addValue(uuid);
        log.trace("the delete sql: {}, {}", sql, tuple.deepToString());
        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, sql, tuple, QUERY_BY_CHAT_UUID_SQL, uuid).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to update data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> updateByMsgUuidDeleted(SqlConnection conn, String userUid, String uuid) {
        String sql = "UPDATE message SET deleted_users = JSON_ARRAY_APPEND(COALESCE(deleted_users, '[]'), '$', '" + userUid +
                "') WHERE message_uuid = ? AND NOT JSON_CONTAINS(COALESCE(deleted_users, '[]'), '" + "\"" + userUid + "\"" + "', '$')";
        Tuple tuple = Tuple.tuple();
        tuple.addValue(uuid);
        log.trace("the delete sql: {}, {}", sql, tuple.deepToString());
        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, sql, tuple, QUERY_BY_MSG_UUID_SQL, uuid).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to update data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<Integer> updateMsgInfoBatch(SqlConnection conn, String time, JsonArray ids) {
        Tuple para = Tuple.tuple();
        StringBuilder updateSql = new StringBuilder(UPDATE_MSG_INFO_BATCH_SQL);
//        updateSql.append(", ").append(ConstDef.MESSAGE_INFO_DATA_KEYS.DELETED_AT).append(" = ?");
        para.addValue(time);
        updateSql.append(" WHERE ").append(ConstDef.MESSAGE_INFO_DATA_KEYS.MESSAGE_UUID);
        String sql = NativeSqlUtils.genInExpSql(updateSql.toString(), ids.size());

        for (int i = 0; i < ids.size(); i++) {
            para.addValue(ids.getString(i));
        }
        Promise<Integer> promise = Promise.promise();
        log.trace("the sql: {}, {}", sql, para.deepToString());
        commonBatchUpdate(conn, sql, para)
                .onSuccess(success -> promise.complete(success.rowCount()))
                .onFailure(failure -> promise.fail(failure.getMessage()));

        return promise.future();
    }




//    public Future<JsonObject> getMsgResourceByMsgUuid(SqlConnection conn, String msgUuid) {
//        Tuple para = Tuple.of(msgUuid);
//        String sql = "SELECT cki.* FROM chat_resource AS cr INNER JOIN chat_knowledge_info AS cki ON cr.knowledge_info_id = cki.id WHERE cr.message_uuid = ? AND cr.is_deleted = 0";
//
//        Promise<JsonObject> promise = Promise.promise();
//        commonQueryInJsonObject(conn, sql, para).onFailure(promise::fail).onSuccess(success -> {
//            JsonArray data = new JsonArray();
//            if (success.size() == 0) {
//                promise.complete(new JsonObject().put("resource_info", data));
//            } else {
//                for (Row value : success) {
//                    data.add(value.toJson());
//                }
//                promise.complete(new JsonObject().put("resource_info", data));
//            }
//        });
//
//        return promise.future();
//    }




    public Future<JsonObject> getMsgInfoByMsgUuid(SqlConnection conn, String msgUuid) {
        Tuple para = Tuple.of(msgUuid);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_MSG_INFO_BY_MSG_UUID_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete(new JsonObject());
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonArray> getMsgByChatUuid(SqlConnection conn, String uuid) {
        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_BY_CHAT_UUID_SQL, Tuple.of(uuid)).onSuccess(success -> {
            JsonArray rt = new JsonArray();
            for (Row row : success)
                rt.add(row.toJson());
            promise.complete(rt);
        }).onFailure(promise::fail);

        return promise.future();
    }


    public Future<JsonArray> getMsgByChatUuidAndMsgUuid(SqlConnection conn, String chatUuid, String msgUuid) {
        Promise<JsonArray> promise = Promise.promise();
        String sql = "SELECT * FROM message WHERE chat_uuid = ? AND message_uuid = ?";
        commonQueryInJsonObject(conn, sql, Tuple.of(chatUuid, msgUuid)).onSuccess(success -> {
            JsonArray rt = new JsonArray();
            for (Row row : success)
                rt.add(row.toJson());
            promise.complete(rt);
        }).onFailure(promise::fail);

        return promise.future();
    }


    public Future<JsonObject> getLatestMsgByChatUuidAndSender(SqlConnection conn, String chatUuid, int senderIdentity) {
        String sql = "SELECT message_content FROM message WHERE chat_uuid = ? AND sender_identity = ? AND message_category = 0 ORDER BY create_time DESC LIMIT 1";
        Tuple para = Tuple.of(chatUuid, senderIdentity);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, sql, para).onSuccess(success -> {
            if (success.size() != 0)
                promise.complete(success.iterator().next().toJson());
            else
                promise.complete(new JsonObject());
        }).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        });

        return promise.future();
    }



    public Future<JsonObject> getLatestMsgByChatUuid(SqlConnection conn, String chatUuid) {
        String sql = "SELECT message_type FROM message WHERE chat_uuid = ? ORDER BY create_time DESC LIMIT 1";
        Tuple para = Tuple.of(chatUuid);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, sql, para).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(new JsonObject());
            else
                promise.complete(success.iterator().next().toJson());
        }).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        });

        return promise.future();
    }


    public Future<JsonObject> getChatRounds(SqlConnection conn, String chatUuid) {
        String sql = "SELECT COUNT(*) AS chat_rounds FROM message WHERE chat_uuid = ? AND (sender_identity = 2 OR sender_identity = 1)";
        Tuple para = Tuple.of(chatUuid);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, sql, para).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(new JsonObject());
            else
                promise.complete(success.iterator().next().toJson());
        }).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        });

        return promise.future();
    }



    public Future<JsonObject> addMsgOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }



    public Future<JsonArray> addMsgBatch(SqlConnection conn, JsonArray rows) {
        Pair<String, List<Tuple>> pairs = genInsertDataByParas(TABLE, rows);
        String insertSql = pairs.getLeft();
        List<Tuple> paras = pairs.getRight();

        Promise<JsonArray> promise = Promise.promise();
        commonBatchAddInJsonObject(conn, insertSql, paras).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(promise::complete);

        return promise.future();
    }


    public Future<JsonArray> addMsgInfoBatch(SqlConnection conn, JsonArray rows) {
        Pair<String, List<Tuple>> pairs = genInsertDataByParas(MESSAGE_INFO_TABLE, rows);
        String insertSql = pairs.getLeft();
        List<Tuple> paras = pairs.getRight();
        log.trace("the addMsgInfoBatch sql: {}, {}", insertSql, paras);

        Promise<JsonArray> promise = Promise.promise();
        commonBatchAddInJsonObject(conn, insertSql, paras).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(promise::complete);

        return promise.future();
    }


    public Future<JsonObject> addMsgInfoOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(MESSAGE_INFO_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_MSG_INFO_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }



    public Future<Pair<Integer, JsonArray>> queryAll(SqlConnection conn, String userUid, List<String> queryParaList, boolean isFilterByDeleted) {
        StringBuilder querySql = new StringBuilder(QUERY_SQL);
        StringBuilder queryCountSql = new StringBuilder(QUERY_COUNT_SQL);

        String countConditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!countConditionSql.isEmpty()) {
            queryCountSql.append(countConditionSql);
        }
        String conditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!conditionSql.isEmpty())
            querySql.append(conditionSql);

        String[] sqlSplit = querySql.toString().split(" WHERE ");
        String[] countSqlSplit = queryCountSql.toString().split(" WHERE ");
        StringBuilder sql = new StringBuilder();
        StringBuilder countSql = new StringBuilder();

        if (isFilterByDeleted) {
            sql.append(sqlSplit[0]).append(" WHERE (").append(sqlSplit[1]).append(")").append(" AND NOT JSON_CONTAINS(COALESCE(deleted_users, '[]'), '" + "\"").append(userUid).append("\"").append("', '$')");
            countSql.append(countSqlSplit[0]).append(" WHERE (").append(countSqlSplit[1]).append(")").append(" AND NOT JSON_CONTAINS(COALESCE(deleted_users, '[]'), '" + "\"").append(userUid).append("\"").append("', '$')");;
        } else {
            sql = querySql;
            countSql = queryCountSql;
        }

        sql.append(" ORDER BY " + ConstDef.CHAT_INFO_DATA_KEYS.CREATE_TIME + " ASC");

        log.debug(sql.toString());
        log.debug(countSql.toString());

        Promise<Pair<Integer, JsonArray>> promise = Promise.promise();
        commonGetPageInJsonObject(conn, queryCountSql.toString(), null, querySql.toString(), null).onFailure(promise::fail).onSuccess(success -> {
            JsonArray jsonArray = new JsonArray();
            for (Row row : success.getRight())
                jsonArray.add(row.toJson());

            promise.complete(Pair.of(success.getLeft(), jsonArray));
        });

        return promise.future();
    }



}

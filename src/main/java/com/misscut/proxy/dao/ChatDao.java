package com.misscut.proxy.dao;

import com.misscut.model.ConstDef;
import com.misscut.utils.TimeUtils;
import com.misscut.utils.mysql.AbstractDao;
import com.misscut.utils.mysql.NativeSqlUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author jiyifei
 * @date 2023/9/26 上午10:54
 */
@Slf4j
public class ChatDao extends AbstractDao {

    public final static String TABLE = "chat_info";
    public final static String KNOWLEDGE_BASIS_TABLE = "chat_knowledge_basis";
    public final static String RESOURCE_TABLE = "chat_resource";
    public final static String USED_KNOWLEDGE_INFO_TABLE = "chat_used_knowledge_info";


    public ChatDao(Pool mySQLClient) {
        super(mySQLClient);
    }

    private final String QUERY_SQL = "SELECT * FROM " + TABLE;
    private final String QUERY_COUNT_SQL = "SELECT count(*) FROM " + TABLE;
    private final String QUERY_BY_ID_SQL = QUERY_SQL + " WHERE " + ConstDef.CHAT_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_BY_CHAT_UUID_SQL = QUERY_SQL + " WHERE " + ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID + " = ?";

//    private final String QUERY_KNOWLEDGE_BASIS_SQL = "SELECT * FROM " + KNOWLEDGE_BASIS_TABLE + " WHERE " + ConstDef.CHAT_KNOWLEDGE_BASIS_DATA_KEYS.IS_DELETED + " = 0";
//    private final String QUERY_KNOWLEDGE_BASIS_COUNT_SQL = "SELECT count(*) FROM " + KNOWLEDGE_BASIS_TABLE + " WHERE " + ConstDef.CHAT_KNOWLEDGE_BASIS_DATA_KEYS.IS_DELETED + " = 0";
//    private final String QUERY_KNOWLEDGE_BASIS_BY_ID_SQL = QUERY_KNOWLEDGE_BASIS_SQL + " AND " + ConstDef.CHAT_KNOWLEDGE_BASIS_DATA_KEYS.ID + " = ?";
//    private final String QUERY_KNOWLEDGE_BASIS_BY_CHAT_UUID_SQL = QUERY_KNOWLEDGE_BASIS_SQL + " AND " + ConstDef.CHAT_KNOWLEDGE_BASIS_DATA_KEYS.CHAT_UUID + " = ?";

    private final String QUERY_RESOURCE_SQL = "SELECT * FROM " + RESOURCE_TABLE + " WHERE " + ConstDef.CHAT_RESOURCE.IS_DELETED + " = 0";
    private final String QUERY_RESOURCE_COUNT_SQL = "SELECT count(*) FROM " + RESOURCE_TABLE + " WHERE " + ConstDef.CHAT_RESOURCE.IS_DELETED + " = 0";
    private final String QUERY_RESOURCE_BY_ID_SQL = QUERY_RESOURCE_SQL + " AND " + ConstDef.CHAT_RESOURCE.ID + " = ?";
    private final String QUERY_RESOURCE_BY_IDS_SQL = QUERY_RESOURCE_SQL + " AND " + ConstDef.CHAT_RESOURCE.ID;
    private final String QUERY_RESOURCE_BY_CHAT_UUID_SQL = QUERY_RESOURCE_SQL + " AND " + ConstDef.CHAT_RESOURCE.CHAT_UUID + " = ?";

    private final String QUERY_USED_KNOWLEDGE_SQL = "SELECT * FROM " + USED_KNOWLEDGE_INFO_TABLE;
    private final String QUERY_USED_KNOWLEDGE_COUNT_SQL = "SELECT count(*) FROM " + USED_KNOWLEDGE_INFO_TABLE;
    private final String QUERY_USED_KNOWLEDGE_BY_ID_SQL = QUERY_USED_KNOWLEDGE_SQL + " WHERE " + ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_USED_KNOWLEDGE_BY_CHAT_UUID_SQL = QUERY_USED_KNOWLEDGE_SQL + "WHERE" + ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.CHAT_UUID + " = ?";


    public Future<JsonArray> addChatResourceBatch(SqlConnection conn, JsonArray rows) {
        Pair<String, List<Tuple>> pairs = genInsertDataByParas(RESOURCE_TABLE, rows);
        String insertSql = pairs.getLeft();
        List<Tuple> paras = pairs.getRight();

        Promise<JsonArray> promise = Promise.promise();
        commonBatchAddInJsonObject(conn, insertSql, paras).onFailure(failure -> {
            promise.fail(failure);
            log.error("[addChatResourceBatch]failed to add data: {}", failure.getMessage());
        }).onSuccess(promise::complete);

        return promise.future();
    }


    public Future<JsonArray> queryByIds(JsonArray ids) {
        String querySql = NativeSqlUtils.genInExpSql(QUERY_RESOURCE_BY_IDS_SQL, ids.size());
        Tuple para = Tuple.tuple();
        for (int i = 0; i < ids.size(); i++) {
            para.addValue(ids.getLong(i));
        }
        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(querySql, para).onSuccess(success -> {
            JsonArray rt = new JsonArray();
            for (Row row : success)
                rt.add(row.toJson());
            promise.complete(rt);
        }).onFailure(failure -> promise.fail(failure.getMessage()));

        return promise.future();
    }


    public Future<JsonObject> getChatInfoByChatUuid(SqlConnection conn, String chatUuid) {
        Tuple para = Tuple.of(chatUuid);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_BY_CHAT_UUID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete(new JsonObject());
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> updateOne(SqlConnection conn, String uuid, JsonObject row) {
        row.remove(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);

        Pair<String, Tuple> pairs = genUpdateDataByPara(TABLE, ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(uuid);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_BY_CHAT_UUID_SQL, uuid).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> updateChatOneDeleted(SqlConnection conn, String userUid, String uuid) {
//        String sql = "UPDATE chat_info SET deleted_users = JSON_ARRAY_APPEND(COALESCE(deleted_users, '[]'), '$', '" + userUid +
//                "') WHERE chat_uuid = ? AND NOT JSON_CONTAINS(COALESCE(deleted_users, '[]'), '" + "\"" + userUid + "\"" + "', '$')";

        String sql = "UPDATE chat_info " +
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




//    public Future<JsonObject> updateKnowledgeBasisOne(SqlConnection conn, String uuid, JsonObject row) {
//        row.remove(ConstDef.CHAT_KNOWLEDGE_BASIS_DATA_KEYS.CHAT_UUID);
//
//        Pair<String, Tuple> pairs = genUpdateDataByPara(KNOWLEDGE_BASIS_TABLE, ConstDef.CHAT_KNOWLEDGE_BASIS_DATA_KEYS.CHAT_UUID, row);
//        String updateSql = pairs.getLeft();
//        Tuple para = pairs.getRight();
//        para.addValue(uuid);
//
//        Promise<JsonObject> promise = Promise.promise();
//        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_KNOWLEDGE_BASIS_BY_CHAT_UUID_SQL, uuid).onFailure(failure -> {
//            promise.fail(failure);
//            log.error("failed to add data: {}", failure.getMessage());
//        }).onSuccess(success -> {
//            if (success.size() == 0) {
//                promise.complete();
//            } else {
//                promise.complete(success.iterator().next().toJson());
//            }
//        });
//
//        return promise.future();
//    }


    public Future<JsonObject> updateResourceByChatUuid(SqlConnection conn, String uuid, JsonObject row) {
        row.remove(ConstDef.CHAT_RESOURCE.CHAT_UUID);

        Pair<String, Tuple> pairs = genUpdateDataByPara(RESOURCE_TABLE, ConstDef.CHAT_RESOURCE.CHAT_UUID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(uuid);

        Promise<JsonObject> promise = Promise.promise();
        commonBatchUpdate(conn, updateSql, para).onFailure(failure -> {
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


    public Future<JsonObject> updateResourceByChatUuidAndResourceId(SqlConnection conn, String uuid, int id) {
        String updateSql = "UPDATE chat_resource SET is_deleted = 1, deleted_at = ? WHERE id = ? AND chat_uuid = ?";
        Tuple para = Tuple.tuple();
        para.addValue(TimeUtils.getTime());
        para.addValue(id);
        para.addValue(uuid);
        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_RESOURCE_BY_CHAT_UUID_SQL, uuid).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonArray> getResourceByChatUuid(SqlConnection conn, String chatUuid) {
        Tuple para = Tuple.of(chatUuid);
        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_RESOURCE_BY_CHAT_UUID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray data = new JsonArray();
            if (success.size() == 0) {
                promise.complete(data);
            } else {
                for (Row value : success) {
                    data.add(value.toJson());
                }
                promise.complete(data);
            }
        });

        return promise.future();
    }


    public Future<JsonObject> getResourceById(SqlConnection conn, Integer id) {
        Tuple para = Tuple.of(id);
        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_RESOURCE_BY_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> updateUsedKnowledge(SqlConnection conn, String uuid, JsonObject row) {
        row.remove(ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.CHAT_UUID);

        Pair<String, Tuple> pairs = genUpdateDataByPara(USED_KNOWLEDGE_INFO_TABLE, ConstDef.CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS.CHAT_UUID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(uuid);

        Promise<JsonObject> promise = Promise.promise();
        commonBatchUpdate(conn, updateSql, para).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> addChatOne(SqlConnection conn, JsonObject row) {
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
        commonGetPageInJsonObject(conn, countSql.toString(), null, sql.toString(), null).onFailure(promise::fail).onSuccess(success -> {
            JsonArray jsonArray = new JsonArray();
            for (Row row : success.getRight())
                jsonArray.add(row.toJson());

            promise.complete(Pair.of(success.getLeft(), jsonArray));
        });

        return promise.future();
    }


    public Future<JsonArray> getChatsBtTimeDiff(SqlConnection conn, int timeDiff) {
        String sql = "SELECT *  FROM chat_info WHERE TIMESTAMPDIFF(HOUR, latest_message_time, NOW()) >= ?";
        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(conn, sql, Tuple.of(timeDiff)).onFailure(promise::fail).onSuccess(success -> {
            JsonArray data = new JsonArray();
            if (success.size() == 0) {
                promise.complete(data);
            } else {
                for (Row value : success) {
                    data.add(value.toJson());
                }
                promise.complete(data);
            }
        });

        return promise.future();
    }

}

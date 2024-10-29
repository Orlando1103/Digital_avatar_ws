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
public class CommonDao extends AbstractDao {

    public final static String REPLY_BASIS_TABLE = "reply_basis";
    public final static String LOGIC_KEY_TABLE = "logic_key";
    public final static String LOGIC_TABLE = "logic";
    public final static String CHAT_USED_KNOWLEDGE_TABLE = "chat_used_knowledge_info";


    public CommonDao(Pool mySQLClient) {
        super(mySQLClient);
    }

    private final String QUERY_REPLY_BASIS_SQL = "SELECT * FROM " + REPLY_BASIS_TABLE;
    private final String QUERY_REPLY_BASIS_COUNT_SQL = "SELECT COUNT(*) FROM " + REPLY_BASIS_TABLE;
    private final String QUERY_REPLY_BASIS_BY_ID_SQL = QUERY_REPLY_BASIS_SQL + " WHERE " + ConstDef.REPLY_BASIS_DATA_KEYS.ID + " = ?";
    private final String QUERY_REPLY_BASIS_BY_OWNER_IDS_SQL = QUERY_REPLY_BASIS_SQL + " WHERE " + ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID;
    private final String QUERY_REPLY_BASIS_BY_OWNER_ID_SQL = QUERY_REPLY_BASIS_SQL + " WHERE " + ConstDef.REPLY_BASIS_DATA_KEYS.OWNER_ID + " = ?";

    private final String QUERY_LOGIC_KEY_SQL = "SELECT * FROM " + LOGIC_KEY_TABLE;
    private final String QUERY_LOGIC_KEY_COUNT_SQL = "SELECT COUNT(*) FROM " + LOGIC_KEY_TABLE;
    private final String QUERY_LOGIC_KEY_BY_ID_SQL = QUERY_LOGIC_KEY_SQL + " WHERE " + ConstDef.LOGIC_KEY_DATA_KEYS.ID + " = ?";

    private final String QUERY_LOGIC_SQL = "SELECT * FROM " + LOGIC_TABLE;
    private final String QUERY_LOGIC_COUNT_SQL = "SELECT COUNT(*) FROM " + LOGIC_TABLE;
    private final String QUERY_LOGIC_BY_ID_SQL = QUERY_LOGIC_SQL + " WHERE " + ConstDef.LOGIC_DATA_KEYS.ID + " = ?";
    private final String QUERY_LOGIC_BY_KEY_ID_SQL = QUERY_LOGIC_SQL + " WHERE " + ConstDef.LOGIC_DATA_KEYS.LOGIC_KEY_ID + " = ?";


    public Future<JsonObject> getOneReplyBasisByOwnerId(String ownerId) {
        Tuple para = Tuple.of(ownerId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(QUERY_REPLY_BASIS_BY_OWNER_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete(new JsonObject());
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<Pair<Integer, JsonArray>> queryAllLogicKey(SqlConnection conn, List<String> queryParaList) {
        StringBuilder querySql = new StringBuilder(QUERY_LOGIC_KEY_SQL);
        StringBuilder queryCountSql = new StringBuilder(QUERY_LOGIC_KEY_COUNT_SQL);

        String countConditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!countConditionSql.isEmpty())
            queryCountSql.append(countConditionSql);

        String conditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!conditionSql.isEmpty())
            querySql.append(conditionSql);

        log.trace(querySql.toString());
        log.trace(queryCountSql.toString());

        Promise<Pair<Integer, JsonArray>> promise = Promise.promise();
        commonGetPageInJsonObject(conn, queryCountSql.toString(), null, querySql.toString(), null).onFailure(promise::fail).onSuccess(success -> {
            JsonArray jsonArray = new JsonArray();
            for (Row row : success.getRight())
                jsonArray.add(row.toJson());

            promise.complete(Pair.of(success.getLeft(), jsonArray));
        });

        return promise.future();
    }


    public Future<JsonArray> getReplyBasisByIds(SqlConnection conn, JsonArray ids) {
        String querySql = NativeSqlUtils.genInExpSql(QUERY_REPLY_BASIS_BY_OWNER_IDS_SQL, ids.size());
        Tuple para = Tuple.tuple();
        for (int i = 0; i < ids.size(); i++) {
            para.addValue(ids.getString(i));
        }
        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(conn, querySql, para).onSuccess(success -> {
            JsonArray rt = new JsonArray();
            for (Row row : success)
                rt.add(row.toJson());
            promise.complete(rt);
        }).onFailure(failure -> promise.fail(failure.getMessage()));

        return promise.future();
    }



    public Future<JsonObject> addUpdateReplyBasis(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(REPLY_BASIS_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        insertSql += " ON DUPLICATE KEY UPDATE profile = VALUES(profile), reply_strategy = VALUES(reply_strategy), event_summary = VALUES(event_summary)";

        log.debug("addUpdateReplyBasis sql: {}", insertSql);

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_REPLY_BASIS_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(new JsonObject());
            else
                promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }

    public Future<JsonArray> addUsedKnowledgeBatch(SqlConnection conn, JsonArray rows) {
        Pair<String, List<Tuple>> pairs = genInsertDataByParas(CHAT_USED_KNOWLEDGE_TABLE, rows);
        String insertSql = pairs.getLeft();
        List<Tuple> paras = pairs.getRight();

        insertSql += " ON DUPLICATE KEY UPDATE frequency = frequency + VALUES(frequency)";

        log.trace("addUsedKnowledgeBatch sql: {}", insertSql);

        Promise<JsonArray> promise = Promise.promise();
        commonBatchAddInJsonObject(conn, insertSql, paras).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(promise::complete);

        return promise.future();
    }


    public Future<JsonObject> addLogicKeyOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(LOGIC_KEY_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        log.debug("sdhfsldf: {}", insertSql);
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_LOGIC_KEY_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }


    public Future<JsonObject> addLogicOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(LOGIC_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        log.debug("234234523: {}", insertSql);

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_LOGIC_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }


    public Future<JsonArray> getLogicByKeyId(SqlConnection conn, int keyId) {
        Tuple para = Tuple.of(keyId);

        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_LOGIC_BY_KEY_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray ja = new JsonArray();
            for (Row row : success)
                ja.add(row.toJson());
            promise.complete(ja);
        });

        return promise.future();
    }


    public Future<JsonArray> getTopNChatUsedKnowledgeByChatUuid(SqlConnection conn, String chatUuid, int topN) {
        Tuple para = Tuple.tuple();
        para.addValue(chatUuid);
        para.addValue(topN);
        String sql = "SELECT knowledge_uuid FROM chat_used_knowledge_info WHERE chat_uuid = ? ORDER BY frequency DESC LIMIT ?";

        Promise<JsonArray> promise = Promise.promise();
        commonQueryInJsonObject(conn, sql, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray ja = new JsonArray();
            for (Row row : success)
                ja.add(row.toJson());
            promise.complete(ja);
        });

        return promise.future();
    }

}

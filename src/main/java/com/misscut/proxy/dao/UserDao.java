package com.misscut.proxy.dao;

import com.misscut.model.ConstDef;
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

/**
 * @author jiyifei
 * @date 2023/9/26 上午10:54
 */
@Slf4j
public class UserDao extends AbstractDao {
    public final static String TABLE = "basis_user_info";
    public final static String PARENT_INFO_TABLE = "parent_info";
    public final static String EXPERT_INFO_TABLE = "expert_info";
    public final static String CHILD_INFO_TABLE = "child_info";
    public final static String FAMILY_INFO_TABLE = "family_info";
    public final static String ORG_INFO_TABLE = "organization_info";
    public final static String ORG_MEMBER_INFO_TABLE = "organization_member_info";


    public UserDao(Pool mySQLClient) {
        super(mySQLClient);
    }
    private final String QUERY_SQL = "SELECT * FROM " + TABLE;
    private final String QUERY_COUNT_SQL = "SELECT count(*) FROM " + TABLE;
    private final String QUERY_BY_ID_SQL = QUERY_SQL + " WHERE " + ConstDef.USER_BASIS_DATA_KEYS.ID + " = ?";
    private final String QUERY_BY_USER_UID_SQL = QUERY_SQL + " WHERE " + ConstDef.USER_BASIS_DATA_KEYS.USER_UID + " = ?";

    private final String QUERY_PARENT_INFO_SQL = "SELECT * FROM " + PARENT_INFO_TABLE;
    private final String QUERY_PARENT_INFO_COUNT_SQL = "SELECT count(*) FROM " + PARENT_INFO_TABLE;
    private final String QUERY_PARENT_INFO_BY_ID_SQL = QUERY_PARENT_INFO_SQL + " WHERE " + ConstDef.PARENT_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_PARENT_INFO_BY_USER_ID_SQL = QUERY_PARENT_INFO_SQL + " WHERE " + ConstDef.PARENT_INFO_DATA_KEYS.USER_ID + " = ?";

    private final String QUERY_EXPERT_INFO_SQL = "SELECT * FROM " + EXPERT_INFO_TABLE;
    private final String QUERY_EXPERT_INFO_COUNT_SQL = "SELECT count(*) FROM " + EXPERT_INFO_TABLE;
    private final String QUERY_EXPERT_INFO_BY_ID_SQL = QUERY_EXPERT_INFO_SQL + " WHERE " + ConstDef.EXPERT_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_EXPERT_INFO_BY_USER_ID_SQL = QUERY_EXPERT_INFO_SQL + " WHERE " + ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID + " = ?";

    private final String QUERY_CHILD_INFO_SQL = "SELECT * FROM " + CHILD_INFO_TABLE;
    private final String QUERY_CHILD_INFO_COUNT_SQL = "SELECT count(*) FROM " + CHILD_INFO_TABLE;
    private final String QUERY_CHILD_INFO_BY_ID_SQL = QUERY_CHILD_INFO_SQL + " WHERE " + ConstDef.CHILD_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_CHILD_INFO_BY_CHILD_ID_SQL = QUERY_CHILD_INFO_SQL + " WHERE " + ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID + " = ?";
    private final String QUERY_CHILD_INFO_BY_PARENT_ID_SQL = QUERY_CHILD_INFO_SQL + " WHERE " + ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID + " = ?";
    private final String DELETE_CHILD_SQL = "DELETE FROM " + CHILD_INFO_TABLE + " WHERE ";

    private final String QUERY_FAMILY_INFO_SQL = "SELECT * FROM " + FAMILY_INFO_TABLE;
    private final String QUERY_FAMILY_INFO_COUNT_SQL = "SELECT count(*) FROM " + FAMILY_INFO_TABLE;
    private final String QUERY_FAMILY_INFO_BY_ID_SQL = QUERY_FAMILY_INFO_SQL + " WHERE " + ConstDef.FAMILY_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_FAMILY_INFO_BY_PARENT_ID_SQL = QUERY_FAMILY_INFO_SQL + " WHERE " + ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID + " = ?";


    private final String QUERY_ORG_INFO_SQL = "SELECT * FROM " + ORG_INFO_TABLE;
    private final String QUERY_ORG_INFO_COUNT_SQL = "SELECT count(*) FROM " + ORG_INFO_TABLE;
    private final String QUERY_ORG_INFO_BY_ID_SQL = QUERY_ORG_INFO_SQL + " WHERE " + ConstDef.ORGANIZATION_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_ORG_INFO_BY_INVITE_CODE_SQL = QUERY_ORG_INFO_SQL + " WHERE " + ConstDef.ORGANIZATION_INFO_DATA_KEYS.INVITE_CODE + " = ?";


    private final String QUERY_ORG_MEMBER_INFO_SQL = "SELECT * FROM " + ORG_MEMBER_INFO_TABLE;
    private final String QUERY_ORG_MEMBER_INFO_COUNT_SQL = "SELECT count(*) FROM " + ORG_MEMBER_INFO_TABLE;
    private final String QUERY_ORG_MEMBER_INFO_BY_ID_SQL = QUERY_ORG_MEMBER_INFO_SQL + " WHERE " + ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.ID + " = ?";
    private final String QUERY_ORG_MEMBER_INFO_BY_MEMBER_ID_SQL = QUERY_ORG_MEMBER_INFO_SQL + " WHERE " + ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_ID + " = ?";
    private final String QUERY_ORG_MEMBER_INFO_BY_ORG_ID_SQL = QUERY_ORG_MEMBER_INFO_SQL + " WHERE " + ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.ORGANIZATION_ID + " = ?";
    private final String DELETE_MEMBER_ORG_SQL = "DELETE FROM " + ORG_MEMBER_INFO_TABLE + " WHERE ";



    public Future<Pair<Integer, JsonArray>> queryByPage(List<String> queryParaList, String sortPara, int offset, int limit) {
        StringBuilder queryCountSql = new StringBuilder(QUERY_COUNT_SQL);
        StringBuilder querySql = new StringBuilder(QUERY_SQL);

        String countConditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!countConditionSql.isEmpty()) {
            queryCountSql.append(countConditionSql);
        }

        String conditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList, sortPara, limit, offset);
        if (!conditionSql.isEmpty())
            querySql.append(conditionSql);

        Promise<Pair<Integer, JsonArray>> promise = Promise.promise();
        commonGetPageInJsonObject(queryCountSql.toString(), null, querySql.toString(), null).onFailure(promise::fail).onSuccess(success -> {
            JsonArray jsonArray = new JsonArray();
            for (Row row : success.getRight())
                jsonArray.add(row.toJson());

            promise.complete(Pair.of(success.getLeft(), jsonArray));
        });

        return promise.future();
    }


    public Future<JsonObject> getOneById(Integer id) {
        Tuple para = Tuple.of(id);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(QUERY_BY_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }



    public Future<JsonObject> getOneByIdConn(SqlConnection conn, Integer id) {
        Tuple para = Tuple.of(id);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_BY_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }



    public Future<JsonObject> getOneByUserUId(String userUid) {
        Tuple para = Tuple.of(userUid);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(QUERY_BY_USER_UID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> getOneByUserUIdConn(SqlConnection conn, String userUid) {
        Tuple para = Tuple.of(userUid);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_BY_USER_UID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> addUserBasisOne(SqlConnection conn, JsonObject row) {
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


    public Future<JsonObject> updateUserBasis(SqlConnection conn, int id, JsonObject row) {
        if (row.isEmpty())
            return Future.succeededFuture();
        row.remove(ConstDef.USER_BASIS_DATA_KEYS.ID);
        Pair<String, Tuple> pairs = genUpdateDataByPara(TABLE, ConstDef.USER_BASIS_DATA_KEYS.ID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(id);

        log.trace("the updateSql: {}, {}, {}", updateSql, QUERY_BY_ID_SQL, id);
        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_BY_ID_SQL, String.valueOf(id)).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }



    public Future<JsonObject> addParentInfoOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(PARENT_INFO_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_PARENT_INFO_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(null);
            else
                promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }


    public Future<JsonObject> addExpertInfoOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(EXPERT_INFO_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_EXPERT_INFO_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(null);
            else
                promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }


    public Future<JsonObject> updateParentInfo(SqlConnection conn, int id, JsonObject row) {
        if (row.isEmpty())
            return Future.succeededFuture();
        Pair<String, Tuple> pairs = genUpdateDataByPara(PARENT_INFO_TABLE, ConstDef.PARENT_INFO_DATA_KEYS.ID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(id);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_PARENT_INFO_BY_ID_SQL, String.valueOf(id)).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> getParentInfoOneByUserId(SqlConnection conn, Integer userId) {
        Tuple para = Tuple.of(userId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_PARENT_INFO_BY_USER_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> getExpertInfoOneByUserId(SqlConnection conn, Integer userId) {
        Tuple para = Tuple.of(userId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_EXPERT_INFO_BY_USER_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }



    public Future<JsonObject> updateExpertInfo(SqlConnection conn, int id, JsonObject row) {
        if (row.isEmpty())
            return Future.succeededFuture();
        Pair<String, Tuple> pairs = genUpdateDataByPara(EXPERT_INFO_TABLE, ConstDef.EXPERT_INFO_DATA_KEYS.ID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(id);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_EXPERT_INFO_BY_ID_SQL, String.valueOf(id)).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }



    public Future<JsonObject> getChildInfoByParentId(SqlConnection conn, Integer parentId) {
        Tuple para = Tuple.of(parentId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_CHILD_INFO_BY_PARENT_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray children = new JsonArray();
            if (success.size() == 0) {
                promise.complete(new JsonObject().put("data", children));
            } else {
                for (Row value : success) {
                    children.add(value.toJson());
                }
                promise.complete(new JsonObject().put("data", children));
            }
        });

        return promise.future();
    }



    public Future<JsonObject> getFamilyInfoOneByParentId(SqlConnection conn, Integer parentId) {
        Tuple para = Tuple.of(parentId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_FAMILY_INFO_BY_PARENT_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete(new JsonObject());
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }



    public Future<JsonObject> addChildInfoOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(CHILD_INFO_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_CHILD_INFO_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(null);
            else
                promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }


    public Future<JsonObject> updateChildInfo(SqlConnection conn, String childId, JsonObject row) {
        Pair<String, Tuple> pairs = genUpdateDataByPara(CHILD_INFO_TABLE, ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(childId);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_CHILD_INFO_BY_CHILD_ID_SQL, childId).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<Integer> deleteChildInfo(SqlConnection conn, JsonObject row) {
        StringBuilder sql = new StringBuilder(DELETE_CHILD_SQL);
        Tuple para = Tuple.tuple();
        for (String field : row.fieldNames()) {
            sql.append(field).append(" = ? AND ");
            para.addValue(row.getValue(field));
        }
        sql.delete(sql.length() - 5, sql.length());
        Promise<Integer> promise = Promise.promise();
        commonDelete(conn, sql.toString(), para)
                .onFailure(promise::fail)
                .onSuccess(promise::complete);

        return promise.future();
    }


    public Future<JsonObject> updateFamilyInfo(SqlConnection conn, int id, JsonObject row) {
        Pair<String, Tuple> pairs = genUpdateDataByPara(FAMILY_INFO_TABLE, ConstDef.FAMILY_INFO_DATA_KEYS.ID, row);
        String updateSql = pairs.getLeft();
        Tuple para = pairs.getRight();
        para.addValue(id);

        Promise<JsonObject> promise = Promise.promise();
        commonUpdateOneInJsonObject(conn, updateSql, para, QUERY_FAMILY_INFO_BY_ID_SQL, String.valueOf(id)).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> addFamilyInfoOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(FAMILY_INFO_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_FAMILY_INFO_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            if (success.size() == 0)
                promise.complete(null);
            else
                promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }


    public Future<JsonObject> getOrgInfoByInviteCode(SqlConnection conn, String code) {
        Tuple para = Tuple.of(code);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_ORG_INFO_BY_INVITE_CODE_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray orgs = new JsonArray();
            if (success.size() == 0) {
                promise.complete(new JsonObject().put("data", orgs));
            } else {
                for (Row value : success) {
                    orgs.add(value.toJson());
                }
                promise.complete(new JsonObject().put("data", orgs));
            }
        });

        return promise.future();
    }


    public Future<JsonObject> getOrgInfoById(SqlConnection conn, int id) {
        Tuple para = Tuple.of(id);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_ORG_INFO_BY_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            if (success.size() == 0) {
                promise.complete();
            } else {
                promise.complete(success.iterator().next().toJson());
            }
        });

        return promise.future();
    }


    public Future<JsonObject> addOrgMemberOne(SqlConnection conn, JsonObject row) {
        Pair<String, Tuple> pairs = genInsertDataByPara(ORG_MEMBER_INFO_TABLE, row);
        String insertSql = pairs.getLeft();
        Tuple para = pairs.getRight();

        Promise<JsonObject> promise = Promise.promise();
        commonAddOneInJsonObject(conn, insertSql, para, QUERY_ORG_MEMBER_INFO_BY_ID_SQL).onFailure(failure -> {
            promise.fail(failure);
            log.error("failed to add data: {}", failure.getMessage());
        }).onSuccess(success -> {
            promise.complete(success.iterator().next().toJson());
        });

        return promise.future();
    }

    public Future<Pair<Integer, JsonArray>> queryOrgMemberByPara(SqlConnection conn, List<String> queryParaList) {
        StringBuilder querySql = new StringBuilder(QUERY_ORG_MEMBER_INFO_SQL);
        StringBuilder queryCountSql = new StringBuilder(QUERY_ORG_MEMBER_INFO_COUNT_SQL);

        String countConditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!countConditionSql.isEmpty()) {
            queryCountSql.append(countConditionSql);
        }
        String conditionSql = NativeSqlUtils.getConditionSql(null, true, queryParaList);
        if (!conditionSql.isEmpty())
            querySql.append(conditionSql);

        log.debug(querySql.toString());
        log.debug(queryCountSql.toString());

        Promise<Pair<Integer, JsonArray>> promise = Promise.promise();
        commonGetPageInJsonObject(conn, queryCountSql.toString(), null, querySql.toString(), null).onFailure(promise::fail).onSuccess(success -> {
            JsonArray jsonArray = new JsonArray();
            for (Row row : success.getRight())
                jsonArray.add(row.toJson());

            promise.complete(Pair.of(success.getLeft(), jsonArray));
        });

        return promise.future();
    }



    public Future<JsonObject> getOrgMemberByMemberId(SqlConnection conn, int memberId) {
        Tuple para = Tuple.of(memberId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_ORG_MEMBER_INFO_BY_MEMBER_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray children = new JsonArray();
            if (success.size() == 0) {
                promise.complete(new JsonObject().put("data", children));
            } else {
                for (Row value : success) {
                    children.add(value.toJson());
                }
                promise.complete(new JsonObject().put("data", children));
            }
        });

        return promise.future();
    }


    public Future<JsonObject> getOrgMemberByOrgId(SqlConnection conn, int memberId) {
        Tuple para = Tuple.of(memberId);

        Promise<JsonObject> promise = Promise.promise();
        commonQueryInJsonObject(conn, QUERY_ORG_MEMBER_INFO_BY_ORG_ID_SQL, para).onFailure(promise::fail).onSuccess(success -> {
            JsonArray children = new JsonArray();
            if (success.size() == 0) {
                promise.complete(new JsonObject().put("data", children));
            } else {
                for (Row value : success) {
                    children.add(value.toJson());
                }
                promise.complete(new JsonObject().put("data", children));
            }
        });

        return promise.future();
    }


    public Future<Integer> deleteMemberInfo(SqlConnection conn, JsonObject row) {
        StringBuilder sql = new StringBuilder(DELETE_MEMBER_ORG_SQL);
        Tuple para = Tuple.tuple();
        for (String field : row.fieldNames()) {
            sql.append(field).append(" = ? AND ");
            para.addValue(row.getValue(field));
        }
        sql.delete(sql.length() - 5, sql.length());
        Promise<Integer> promise = Promise.promise();
        commonDelete(conn, sql.toString(), para)
                .onFailure(promise::fail)
                .onSuccess(promise::complete);

        return promise.future();
    }


    public Future<JsonArray> getAllExperts(SqlConnection conn) {
        Promise<JsonArray> promise = Promise.promise();
        String sql = "SELECT m.user_uid as expert_uuid, n.field, n.level, n.seniority FROM basis_user_info AS m INNER JOIN expert_info AS n ON m.id = n.user_id WHERE m.user_identity = 1";
        commonQueryInJsonObject(conn, sql, Tuple.tuple()).onFailure(promise::fail).onSuccess(success -> {
            JsonArray ja = new JsonArray();
            for (Row value : success) {
                ja.add(value.toJson());
            }
            promise.complete(ja);
        });

        return promise.future();
    }
}

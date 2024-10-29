//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.misscut.utils.mysql;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.*;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractDao {
    private static final Logger log = LoggerFactory.getLogger(AbstractDao.class);
    protected Pool mySQLClient;

    public AbstractDao() {
    }

    public AbstractDao(Pool mySQLClient) {
        this.mySQLClient = mySQLClient;
    }

    protected Pair<String, Tuple> genInsertDataByPara(String tableName, JsonObject row) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(tableName)
                .append(" (");

        Tuple para = Tuple.tuple();
        for (String field : row.fieldNames()) {
            sql.append(field).append(", ");
            para.addValue(row.getValue(field));
        }

        sql.delete(sql.length()-2, sql.length());
        sql.append(") ")
                .append("VALUES ")
                .append("(");

        sql.append("?, ".repeat(Math.max(0, row.size())));
        sql.delete(sql.length()-2, sql.length());
        sql.append(")");

        return Pair.of(sql.toString(), para);
    }

    protected Pair<String, List<Tuple>> genInsertDataByParas(String tableName, JsonArray rows) {
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ")
                .append(tableName)
                .append(" (");

        JsonObject row = rows.getJsonObject(0);
        for (String field : row.fieldNames()) {
            sql.append(field).append(", ");
        }
        sql.delete(sql.length()-2, sql.length());
        sql.append(") ")
                .append("VALUES ")
                .append("(");

        sql.append("?, ".repeat(Math.max(0, row.size())));
        sql.delete(sql.length()-2, sql.length());
        sql.append(")");

        List<Tuple> paras = new ArrayList<>();
        for (int i=0; i<rows.size(); i++) {
            Tuple para = Tuple.tuple();

            JsonObject data = rows.getJsonObject(i);
            for (String field : data.fieldNames()) {
                para.addValue(data.getValue(field));
            }
            paras.add(para);
        }

        return Pair.of(sql.toString(), paras);
    }

    protected Pair<String, Tuple> genUpdateDataByPara(String tableName, String idColName, JsonObject row) {
        StringBuilder sql = new StringBuilder();
        sql.append("UPDATE ")
                .append(tableName)
                .append(" SET ");

        Tuple para = Tuple.tuple();
        for (String field : row.fieldNames()) {
            sql.append(field).append(" = ?, ");
            para.addValue(row.getValue(field));
        }

        sql.delete(sql.length()-2, sql.length());
        sql.append(" WHERE ").append(idColName).append(" = ?");

        return Pair.of(sql.toString(), para);
    }

    protected Future<RowSet<Row>> commonAddOneInJsonObject(String insertSql, Tuple para, String queryByIdSql) {
        Promise<RowSet<Row>> promise = Promise.promise();

        this.mySQLClient.getConnection().compose(conn -> {
            return conn
                    .preparedQuery(insertSql)
                    .execute(para)
                    .compose(rows -> {
                        long lastInsertId = rows.property(PropertyKind.create("last-inserted-id", Long.class));
                        return conn.preparedQuery(queryByIdSql)
                                .execute(Tuple.of(lastInsertId));
                    }).onComplete(done -> {
                        conn.close();
                    });
        }).onFailure(promise::fail)
                .onSuccess(promise::complete);

        return promise.future();
    }

    protected Future<RowSet<Row>> commonAddOneInJsonObject(SqlConnection conn, String insertSql, Tuple para, String queryByIdSql) {
        Promise<RowSet<Row>> promise = Promise.promise();

        conn.preparedQuery(insertSql)
                            .execute(para)
                            .compose(rows -> {
                                long lastInsertId = rows.property(PropertyKind.create("last-inserted-id", Long.class));
                                return conn.preparedQuery(queryByIdSql)
                                        .execute(Tuple.of(lastInsertId));
                }).onFailure(promise::fail)
                .onSuccess(promise::complete);

        return promise.future();
    }

    protected Future<JsonArray> commonBatchAddInJsonObject(String insertSql, List<Tuple> paras) {
        Promise<JsonArray> promise = Promise.promise();

        this.mySQLClient.getConnection().compose(conn -> {
                    return conn
                            .preparedQuery(insertSql)
                            .executeBatch(paras).onComplete(done-> {
                                conn.close();
                            });
                }).onFailure(promise::fail)
                .onSuccess(success -> {
                    PropertyKind<Long> propertyKind = PropertyKind.create("last-inserted-id", Long.class);
                    JsonArray insertedIds = new JsonArray();

                    RowSet<Row> rs = success;
                    for (int i=0; i<paras.size(); i++) {
                        if (rs == null)
                            break;

                        insertedIds.add(rs.property(propertyKind));
                        rs = rs.next();
                    }

                    promise.complete(insertedIds);
                });

        return promise.future();
    }

    protected Future<JsonArray> commonBatchAddInJsonObject(SqlConnection conn, String insertSql, List<Tuple> paras) {
        Promise<JsonArray> promise = Promise.promise();

        conn.preparedQuery(insertSql)
                            .executeBatch(paras).onFailure(promise::fail)
                .onSuccess(success -> {
                    PropertyKind<Long> propertyKind = PropertyKind.create("last-inserted-id", Long.class);
                    JsonArray insertedIds = new JsonArray();

                    RowSet<Row> rs = success;
                    for (int i=0; i<paras.size(); i++) {
                        if (rs == null)
                            break;

                        insertedIds.add(rs.property(propertyKind));
                        rs = rs.next();
                    }

                    promise.complete(insertedIds);
                });

        return promise.future();
    }

    protected Future<RowSet<Row>> commonUpdateOneInJsonObject(String updateSql, Tuple para, String queryByIdSql, String id) {
        Promise<RowSet<Row>> promise = Promise.promise();
        this.mySQLClient.getConnection().compose(conn -> conn
                .preparedQuery(updateSql)
                .execute(para)
                .compose(rows -> {
                    return conn.preparedQuery(queryByIdSql)
                            .execute(Tuple.of(id));
                }).onComplete(done -> {
                    conn.close();
                })).onFailure(promise::fail).onSuccess(promise::complete);

        return promise.future();
    }


    protected Future<RowSet<Row>> commonBatchUpdate(String updateSql, Tuple para) {
        Promise<RowSet<Row>> promise = Promise.promise();
        this.mySQLClient.getConnection().compose(conn -> conn
                .preparedQuery(updateSql)
                .execute(para)
                .onComplete(done -> {
                    conn.close();
                })).onFailure(promise::fail).onSuccess(promise::complete);

        return promise.future();
    }

    protected Future<RowSet<Row>> commonBatchUpdate(SqlConnection conn, String updateSql, Tuple para) {
        Promise<RowSet<Row>> promise = Promise.promise();
        conn.preparedQuery(updateSql)
                .execute(para).onFailure(promise::fail).onSuccess(promise::complete);

        return promise.future();
    }


    protected Future<RowSet<Row>> commonUpdateOneInJsonObject(SqlConnection conn, String updateSql, Tuple para, String queryByIdSql, String id) {
        Promise<RowSet<Row>> promise = Promise.promise();
        conn.preparedQuery(updateSql)
                    .execute(para)
                    .compose(rows -> {
                        return conn.preparedQuery(queryByIdSql)
                                .execute(Tuple.of(id));
                    }).onFailure(promise::fail).onSuccess(promise::complete);

        return promise.future();
    }

    protected Future<RowSet<Row>> commonQueryInJsonObject(String querySql, Tuple queryPara) {
        Promise<RowSet<Row>> promise = Promise.promise();

        this.mySQLClient.getConnection().compose(conn -> {
            Future<RowSet<Row>> future = queryPara != null ? conn.preparedQuery(querySql).execute(queryPara) : conn.preparedQuery(querySql).execute();
            future.onComplete(done -> {
                conn.close();
            });

            return future;
        }).onFailure(promise::fail).onSuccess(promise::complete);

        return promise.future();
    }

    protected Future<RowSet<Row>> commonQueryInJsonObject(SqlConnection conn, String querySql, Tuple queryPara) {
        Promise<RowSet<Row>> promise = Promise.promise();
        Future<RowSet<Row>> future = queryPara != null ? conn.preparedQuery(querySql).execute(queryPara) : conn.preparedQuery(querySql).execute();
        future.onFailure(promise::fail).onSuccess(promise::complete);
        return promise.future();
    }

    protected Future<Pair<Integer, RowSet<Row>>> commonGetPageInJsonObject(String queryCountSql, Tuple queryCountPara, String querySql, Tuple queryPara) {
        Promise<Pair<Integer, RowSet<Row>>> promise = Promise.promise();

        this.mySQLClient.getConnection().compose(conn -> {
            Future<RowSet<Row>> future1 = queryCountPara != null ? conn.preparedQuery(queryCountSql).execute(queryCountPara) : conn.preparedQuery(queryCountSql).execute();
            Future<RowSet<Row>> future2 = queryPara != null ? conn.preparedQuery(querySql).execute(queryPara) : conn.preparedQuery(querySql).execute();

            return CompositeFuture.all(future1, future2).onComplete(done -> {
                conn.close();
            });
        }).onFailure(promise::fail).onSuccess(success -> {
            int size = ((RowSet<Row>)success.resultAt(0)).iterator().next().get(Integer.class, 0);
            RowSet<Row> results = success.resultAt(1);

            promise.complete(Pair.of(size, results));
        });

        return promise.future();
    }

    protected Future<Pair<Integer, RowSet<Row>>> commonGetPageInJsonObject(SqlConnection conn, String queryCountSql, Tuple queryCountPara, String querySql, Tuple queryPara) {
        Promise<Pair<Integer, RowSet<Row>>> promise = Promise.promise();

        Future<RowSet<Row>> future1 = queryCountPara != null ? conn.preparedQuery(queryCountSql).execute(queryCountPara) : conn.preparedQuery(queryCountSql).execute();
        Future<RowSet<Row>> future2 = queryPara != null ? conn.preparedQuery(querySql).execute(queryPara) : conn.preparedQuery(querySql).execute();

        CompositeFuture.all(future1, future2).onFailure(promise::fail).onSuccess(success -> {
            int size = ((RowSet<Row>)success.resultAt(0)).iterator().next().get(Integer.class, 0);
            RowSet<Row> results = success.resultAt(1);

            promise.complete(Pair.of(size, results));
        });

        return promise.future();
    }

    protected Future<Integer> commonDelete(String delSql, Tuple para) {
        Promise<Integer> promise = Promise.promise();

        this.mySQLClient.getConnection().compose(conn -> {
            return conn
                    .preparedQuery(delSql)
                    .execute(para)
                    .onComplete(done -> {
                        conn.close();
                    });
        }).onFailure(promise::fail).onSuccess(success -> {
            promise.complete(success.rowCount());
        });

        return promise.future();
    }

    protected Future<Integer> commonDelete(SqlConnection conn, String delSql, Tuple para) {
        Promise<Integer> promise = Promise.promise();

        conn.preparedQuery(delSql)
                    .execute(para).onFailure(promise::fail).onSuccess(success -> {
            promise.complete(success.rowCount());
        });

        return promise.future();
    }
}

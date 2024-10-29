package com.misscut.proxy;

import com.futureinteraction.utils.GetParaParser;
import com.futureinteraction.utils.RedisHolder;
import com.futureinteraction.utils.StringUtils;
import com.misscut.event.ErrorCodes;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.proxy.dao.UserDao;
import com.misscut.utils.CommonUtils;
import com.misscut.utils.EventUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import io.vertx.sqlclient.Pool;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


@Slf4j
public class UserProxy {
    private final Pool mySQLClient;
    private RedisHolder redis;
    private final UserDao userDao;
    private WebClient webClient;
    private Vertx vertx;

    public UserProxy(Pool mySQLClient, RedisHolder redis, WebClient webClient, Vertx vertx) {
        this.mySQLClient = mySQLClient;
        this.redis = redis;
        this.userDao = new UserDao(mySQLClient);
        this.webClient = webClient;
        this.vertx = vertx;
    }

    public void proc(Message<JsonObject> msg) {
        String action = msg.headers().get(BasicEventProcProxy.ACTION);

        switch (action) {
            case (EventConst.USER.ACTIONS.QUERY_BY_PAGE) -> queryByPage(msg);
            case (EventConst.USER.ACTIONS.ADD) -> add(msg);
            case (EventConst.USER.ACTIONS.UPDATE) -> update(msg);
            case (EventConst.USER.ACTIONS.UPDATE_CHILD) -> updateChild(msg);
            case (EventConst.USER.ACTIONS.DELETE_CHILD) -> deleteChild(msg);
            case (EventConst.USER.ACTIONS.UPDATE_FAMILY) -> updateFamily(msg);
            case (EventConst.USER.ACTIONS.QUERY_BY_ID) -> queryById(msg);
            case (EventConst.USER.ACTIONS.QUERY_BY_USER_UID) -> queryByUserUId(msg);
            case (EventConst.USER.ACTIONS.QUERY_PARENT_INFO_BY_USER_ID) -> queryParentInfoByUserId(msg);
            case (EventConst.USER.ACTIONS.QUERY_EXPERT_INFO_BY_USER_ID) -> queryExpertInfoByUserId(msg);
            case (EventConst.USER.ACTIONS.QUERY_ALL_EXPERTS) -> queryAllExperts(msg);
            case (EventConst.USER.ACTIONS.QUERY_FAMILY_INFO_BY_PARENT_ID) -> queryFamilyInfoByParentId(msg);
            case (EventConst.USER.ACTIONS.QUERY_CHILD_INFO_BY_PARENT_ID) -> queryChildInfoByParentId(msg);

            case (EventConst.USER.ACTIONS.QUERY_ORG_BY_INVITE_CODE) -> queryOrgByInviteCode(msg);
            case (EventConst.USER.ACTIONS.QUERY_ORG_BY_ID) -> queryOrgById(msg);
            case (EventConst.USER.ACTIONS.ADD_ORG_MEMBER) -> addOrgMember(msg);
            case (EventConst.USER.ACTIONS.QUERY_ORG_MEMBER_BY_MEMBER_ID) -> queryOrgMemberByMemberId(msg);
            case (EventConst.USER.ACTIONS.QUERY_ORG_MEMBER_BY_ORG_ID) -> queryOrgMemberByOrgId(msg);
//            case (EventConst.USER.ACTIONS.QUERY_ORG_MEMBER_BY_PARA) -> queryOrgMemberByPara(msg);
            case (EventConst.USER.ACTIONS.DELETE_ORG) -> deleteMemberOrg(msg);


            default -> msg.fail(ErrorCodes.INVALID_ACTION, "invalid action: " + action);
        }
    }


    private void queryAllExperts(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getAllExperts(conn)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[deleteMemberOrg]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void deleteMemberOrg(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.deleteMemberInfo(conn, body)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[deleteMemberOrg]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryOrgById(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer orgId = body.getInteger(ConstDef.ORGANIZATION_INFO_DATA_KEYS.ID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getOrgInfoById(conn, orgId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryOrgMemberByOrgId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryOrgMemberByOrgId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer orgId = body.getInteger(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.ORGANIZATION_ID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getOrgMemberByOrgId(conn, orgId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryOrgMemberByOrgId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryOrgMemberByMemberId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer memberId = body.getInteger(ConstDef.ORGANIZATION_MEMBER_INFO_DATA_KEYS.MEMBER_ID);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getOrgMemberByMemberId(conn, memberId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryOrgMemberByMemberId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryOrgMemberByPara(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        List<String> queryParas = new ArrayList<>();

        for (String fieldName : body.fieldNames()) {
            queryParas = CommonUtils.updateParaList(queryParas, fieldName, body.getValue(fieldName), ":=:");
        }

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.queryOrgMemberByPara(conn, finalQueryParas)
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


    private void addOrgMember(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        List<String> queryParas = new ArrayList<>();

        for (String fieldName : body.fieldNames()) {
            queryParas = CommonUtils.updateParaList(queryParas, fieldName, body.getValue(fieldName), ":=:");
        }

        List<String> finalQueryParas = queryParas;
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.queryOrgMemberByPara(conn, finalQueryParas)
                                .compose(members -> {
                                    if (members.getLeft() >= 1) {
                                        return Future.failedFuture("已加入该组织");
                                    }
                                    return userDao.addOrgMemberOne(conn, body);

                                }).compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                                .eventually(v -> conn.close())
                                .onFailure(failure -> {
                                    log.error("[addOrgMember]Transaction failed: {}", failure.getMessage());
                                    msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                                }).onSuccess(v -> {
                                    log.trace("Transaction succeeded");
                                    msg.reply(v);
                                }));
    }


    private void queryOrgByInviteCode(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        String inviteCode = body.getString(ConstDef.ORGANIZATION_INFO_DATA_KEYS.INVITE_CODE);

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getOrgInfoByInviteCode(conn, inviteCode)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryOrgByInviteCode]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryById(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        userDao.getOneById(body.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID))
                .onSuccess(msg::reply)
                .onFailure(fail -> msg.fail(ErrorCodes.DB_ERROR, fail.getMessage()));
    }


    private void queryByUserUId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        userDao.getOneByUserUId(body.getString(ConstDef.USER_BASIS_DATA_KEYS.USER_UID))
                .onSuccess(msg::reply)
                .onFailure(fail -> msg.fail(ErrorCodes.DB_ERROR, fail.getMessage()));
    }


    private void queryExpertInfoByUserId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer userId = body.getInteger(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID);
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getExpertInfoOneByUserId(conn, userId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryExpertInfoByUserId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryFamilyInfoByParentId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer parentId = body.getInteger(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID);
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getFamilyInfoOneByParentId(conn, parentId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryFamilyInfoByParentId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryChildInfoByParentId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer parentId = body.getInteger(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID);
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getChildInfoByParentId(conn, parentId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryChildInfoByParentId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryParentInfoByUserId(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        Integer userId = body.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID);
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getParentInfoOneByUserId(conn, userId)
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[queryParentInfoByUserId]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));
    }


    private void queryByPage(Message<JsonObject> msg) {
        JsonObject body = msg.body();
        GetParaParser.ListQueryPara para = EventUtils.parseQueryPara(body, EventUtils.QUERY_PARA);
        userDao.queryByPage(para.queryParaList, para.sortPara, para.offset, para.limit).onSuccess(success -> {
            Integer left = success.getLeft();
            JsonArray right = success.getRight();
            JsonObject data = new JsonObject().put("data", new JsonArray().add(left).add(right));
            msg.reply(data);
        }).onFailure(failure -> {
            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
        });
    }


    private void add(Message<JsonObject> msg) {
        JsonObject body = msg.body();

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.addUserBasisOne(conn, body)
                                .compose(user -> {
                                    Integer identity = user.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);
                                    Integer userId = user.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
                                    Promise<Pair<JsonObject, JsonObject>> promise = Promise.promise();
                                    JsonObject para = new JsonObject();
                                    if (identity == ConstDef.USER_IDENTITY.PARENT) {
                                        para.put(ConstDef.PARENT_INFO_DATA_KEYS.USER_ID, userId);
                                        userDao.addParentInfoOne(conn, para).onComplete(done -> promise.complete(Pair.of(user, done.result())));
                                    } else {
                                        para.put(ConstDef.EXPERT_INFO_DATA_KEYS.USER_ID, userId);
                                        userDao.addExpertInfoOne(conn, para).onComplete(done -> promise.complete(Pair.of(user, done.result())));
                                    }

                                    return promise.future();
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
                            JsonObject rt = new JsonObject().put("user_basis", v.getLeft()).put("parent_or_expert", v.getRight());
                            msg.reply(rt);
                        }));
    }


    private void update(Message<JsonObject> msg) {
        JsonObject data = msg.body();

        Integer id = data.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        Integer identity = data.getInteger(ConstDef.USER_BASIS_DATA_KEYS.USER_IDENTITY);

        JsonObject basisPara = new JsonObject();
        JsonObject parentInfoPara = new JsonObject();
        JsonObject expertInfoPara = new JsonObject();

        if (identity == ConstDef.USER_IDENTITY.PARENT) {
            if (data.containsKey("parent_name"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME, data.getString("parent_name"));
            if (data.containsKey("parent_name"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_AGE, data.getInteger("parent_age"));
            if (data.containsKey("parent_sex"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_SEX, data.getInteger("parent_sex"));
            if (data.containsKey("avatar_path"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR, data.getString("avatar_path"));

            if (data.containsKey("education_level"))
                parentInfoPara.put(ConstDef.PARENT_INFO_DATA_KEYS.EDUCATION_LEVEL, data.getString("education_level"));
            if (data.containsKey("employment"))
                parentInfoPara.put(ConstDef.PARENT_INFO_DATA_KEYS.EMPLOYMENT, data.getString("employment"));
            if (data.containsKey("tag"))
                parentInfoPara.put(ConstDef.PARENT_INFO_DATA_KEYS.TAG, data.getString("tag"));
        } else {
            if (data.containsKey("expert_name"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_NAME, data.getString("expert_name"));
            if (data.containsKey("expert_sex"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_SEX, data.getInteger("expert_sex"));
            if (data.containsKey("avatar_path"))
                basisPara.put(ConstDef.USER_BASIS_DATA_KEYS.USER_AVATAR, data.getString("avatar_path"));

            if (data.containsKey("field"))
                expertInfoPara.put(ConstDef.EXPERT_INFO_DATA_KEYS.FIELD, data.getString("field"));
            if (data.containsKey("level"))
                expertInfoPara.put(ConstDef.EXPERT_INFO_DATA_KEYS.LEVEL, data.getString("level"));
            if (data.containsKey("seniority"))
                expertInfoPara.put(ConstDef.EXPERT_INFO_DATA_KEYS.SENIORITY, data.getString("seniority"));
        }

        log.debug("the body: {}, useBasis: {}, parentInfo: {}, expertInfo: {}", data, basisPara, parentInfoPara, expertInfoPara);
        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.updateUserBasis(conn, id, basisPara)
                                .compose(user -> {
                                    if (identity == ConstDef.USER_IDENTITY.PARENT)
                                        return userDao.getParentInfoOneByUserId(conn, id);
                                    else
                                        return userDao.getExpertInfoOneByUserId(conn, id);
                                })
                                .compose(parentOrExpertInfo -> {
                                    if (identity == ConstDef.USER_IDENTITY.PARENT) {
                                        Integer parentId = parentOrExpertInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);
                                        return userDao.updateParentInfo(conn, parentId, parentInfoPara);
                                    } else {
                                        Integer expertId = parentOrExpertInfo.getInteger(ConstDef.EXPERT_INFO_DATA_KEYS.ID);
                                        return userDao.updateExpertInfo(conn, expertId, expertInfoPara);
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


    private void updateChild(Message<JsonObject> msg) {
        JsonObject data = msg.body();

        Integer id = data.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        String childId = data.getString("child_id");
        JsonObject para = new JsonObject()
                .put(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_NAME, data.getString("child_name"))
                .put(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_SEX, data.getInteger("child_sex"))
                .put(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_AGE, data.getInteger("child_age"))
                .put(ConstDef.CHILD_INFO_DATA_KEYS.MENTAL_HEALTH, data.getString("mental_health"))
                .put(ConstDef.CHILD_INFO_DATA_KEYS.EDUCATION_LEVEL, data.getString("education_level"))
                .put(ConstDef.CHILD_INFO_DATA_KEYS.ACADEMIC_RECORD, data.getString("academic_record"));

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getParentInfoOneByUserId(conn, id)
                                .compose(parentInfo -> {
                                    Integer parentId = parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);
                                    if (StringUtils.isNullOrEmpty(childId)) {
                                        para.put(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID, parentId);
                                        para.put(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID, UUID.randomUUID() + "-c");
                                        return userDao.addChildInfoOne(conn, para);
                                    } else {
                                        return userDao.updateChildInfo(conn, childId, para);
                                    }
                                })
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[updateChild]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));

    }


    private void deleteChild(Message<JsonObject> msg) {
        JsonObject data = msg.body();

        Integer id = data.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        JsonObject para = new JsonObject().put(ConstDef.CHILD_INFO_DATA_KEYS.CHILD_ID, data.getString("child_id"));

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getParentInfoOneByUserId(conn, id)
                                .compose(parentInfo -> {
                                    Integer parentId = parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);
                                    para.put(ConstDef.CHILD_INFO_DATA_KEYS.PARENT_ID, parentId);
                                    return userDao.deleteChildInfo(conn, para);
                                })
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[deleteChild]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));


    }


    private void updateFamily(Message<JsonObject> msg) {
        JsonObject data = msg.body();

        Integer id = data.getInteger(ConstDef.USER_BASIS_DATA_KEYS.ID);
        JsonObject para = new JsonObject()
                .put(ConstDef.FAMILY_INFO_DATA_KEYS.FAMILY_TYPE, data.getString("family_type"))
                .put(ConstDef.FAMILY_INFO_DATA_KEYS.ACCOMPANY, data.getString("accompany"))
                .put(ConstDef.FAMILY_INFO_DATA_KEYS.COMMUNICATE, data.getString("communicate"))
                .put(ConstDef.FAMILY_INFO_DATA_KEYS.RELATIONSHIP, data.getString("relationship"));

        mySQLClient.getConnection().onSuccess(conn ->
                conn.begin()
                        .compose(tx -> userDao.getParentInfoOneByUserId(conn, id)
                                .compose(parentInfo -> {
                                    Integer parentId = parentInfo.getInteger(ConstDef.PARENT_INFO_DATA_KEYS.ID);
                                    Promise<Pair<Integer, JsonObject>> promise = Promise.promise();
                                    userDao.getFamilyInfoOneByParentId(conn, parentId).onSuccess(success -> {
                                        promise.complete(Pair.of(parentId, success));
                                    }).onFailure(failure -> {
                                        promise.fail(failure.getMessage());
                                    });

                                    return promise.future();
                                })
                                .compose(family -> {
                                    Integer parentId = family.getLeft();
                                    JsonObject familyInfo = family.getRight();
                                    if (familyInfo != null && !familyInfo.isEmpty()) {
                                        return userDao.updateFamilyInfo(conn, familyInfo.getInteger(ConstDef.FAMILY_INFO_DATA_KEYS.ID), para);
                                    } else {
                                        para.put(ConstDef.FAMILY_INFO_DATA_KEYS.PARENT_ID, parentId);
                                        return userDao.addFamilyInfoOne(conn, para);
                                    }
                                })
                                .compose(v1 -> {
                                    tx.commit();
                                    return Future.succeededFuture(v1);
                                }))
                        .eventually(v -> conn.close())
                        .onFailure(failure -> {
                            log.error("[updateFamily]Transaction failed: {}", failure.getMessage());
                            msg.fail(ErrorCodes.DB_ERROR, failure.getMessage());
                        }).onSuccess(v -> {
                            log.trace("Transaction succeeded");
                            msg.reply(v);
                        }));

    }

}

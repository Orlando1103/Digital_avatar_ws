package com.misscut.model;

/**
 * @Author WangWenTao
 * @Date 2024-08-06 10:13:46
 **/
public interface RedisKeyConstDef {

    interface USER {
        String CHECK_NO = "user.check-no.";
        String AUTH = "user.auth-data.";
        String PRESENCE_STATUS = "user.presence-status.";
    }

    interface LOGIN_TYPE {
        String WEB = "web";
        String APP = "app";
    }
}

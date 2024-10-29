package com.misscut.utils;

import io.vertx.core.json.JsonObject;

public class ResponseUtils {

    public static <T> JsonObject response(Integer code, String msg, T data) {
        return new JsonObject()
                .put("code", code)
                .put("msg", msg)
                .put("result", data);
    }

}

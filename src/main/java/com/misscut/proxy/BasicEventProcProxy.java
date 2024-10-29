package com.misscut.proxy;


import com.google.gson.Gson;
import com.misscut.event.ErrorCodes;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;

public abstract class BasicEventProcProxy {

    public static String ACTION = "action";
    protected Gson gson = new Gson();

    public abstract void proc(Message<JsonObject> msg);

    protected void onFailure(Message<JsonObject> msg, Throwable cause, Logger logger) {
        msg.fail(ErrorCodes.DB_ERROR, cause.getMessage());
        logger.error(cause.getMessage());
    }

    protected void onFailure(Message<JsonObject> msg, int error_code, String message, Logger logger) {
        msg.fail(error_code, message);
        logger.info(msg.body().encodePrettily());
    }
}

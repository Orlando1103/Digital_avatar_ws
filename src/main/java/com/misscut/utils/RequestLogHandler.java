package com.misscut.utils;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.LoggerFormat;

public interface RequestLogHandler extends Handler<RoutingContext> {
    static RequestLogHandler create() {
        return new Slf4jRequestLogger(LoggerFormat.SHORT);
    }

    static RequestLogHandler create(LoggerFormat format) {
        return new Slf4jRequestLogger(format);
    }
}
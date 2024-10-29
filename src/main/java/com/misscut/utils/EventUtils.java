package com.misscut.utils;

import com.futureinteraction.utils.GetParaParser;
import com.google.gson.Gson;
import com.misscut.event.EventConst;
import io.vertx.core.json.JsonObject;

public class EventUtils {
    private static Gson gson = new Gson();

    public static String QUERY_PARA = "query-para";

    public static GetParaParser.ListQueryPara parseQueryPara(JsonObject para, String paraKey) {
        return para.containsKey(paraKey) ?
                gson.fromJson(para.getString(QUERY_PARA), GetParaParser.ListQueryPara.class)
                : new GetParaParser.ListQueryPara();
    }
}

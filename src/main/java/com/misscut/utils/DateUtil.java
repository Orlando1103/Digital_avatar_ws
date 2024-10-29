package com.misscut.utils;

import com.futureinteraction.utils.StringUtils;
import com.hazelcast.splitbrainprotection.PingAware;
import com.misscut.model.ConstDef;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.tuple.Pair;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jiyifei
 * @date 2023/10/9 上午9:22
 */
public class DateUtil {
    private static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");


    public static Long DateToTimestamp(String dateString) {
        // 解析日期字符串为 LocalDate 对象
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy年M月d日");
        LocalDate date = LocalDate.parse(dateString, formatter);

        // 将 LocalDate 转换为 Unix 时间戳
        Instant instant = date.atStartOfDay(ZoneId.systemDefault()).toInstant();

        return instant.toEpochMilli();
    }


    public static String getDate() {
        LocalDate localDate = LocalDate.now();
        LocalDateTime dateTime = localDate.atStartOfDay(); // 转换为当天的午夜

        ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
        ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);

        return zonedDateTime.format(dateFormatter);
    }


    public static String getTime() {
        LocalDateTime localDateTime = LocalDateTime.now();
        return localDateTime.format(dateTimeFormatter);
    }


    public static boolean isBeforeDate(String dateStr1, String dateStr2) {
        if (StringUtils.isNullOrEmpty(dateStr1) || "null".equals(dateStr1))
            dateStr1 = getDate();
        if (StringUtils.isNullOrEmpty(dateStr2) || "null".equals(dateStr2))
            dateStr2 = getDate();

        LocalDate date1 = LocalDate.parse(dateStr1, dateFormatter);
        LocalDate date2 = LocalDate.parse(dateStr2, dateFormatter);

        return date1.isBefore(date2) || date1.equals(date2);
    }

    public static String getDateFromTimestamp(Long timestamp) {
        if (timestamp == null)
            return "";
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        LocalDate localDate = localDateTime.toLocalDate();

        return localDate.format(dateFormatter);
    }


    public static Pair<String, JsonObject> findMaxTime(List<JsonObject> data, String timeKey) {
        try {
            if (data.isEmpty()) {
                return Pair.of(null, null);
            }
            String maxTime = "";
            JsonObject curData = data.get(0);
            for (JsonObject obj : data) {
                String currentTime = obj.getString(timeKey);
                Long timestamp = TimeParserUtils.parseDate(currentTime, TimeParserUtils.getInstance().getDateRegexes());
                currentTime = getDateFromTimestamp(timestamp);

                if (!StringUtils.isNullOrEmpty(currentTime) && (maxTime.isEmpty() || currentTime.compareTo(maxTime) > 0)) {
                    maxTime = currentTime;
                    curData = obj;
                }
            }

            return Pair.of(maxTime, curData);
        } catch (Exception e) {
            return Pair.of(null, data.get(0));
        }
    }
}

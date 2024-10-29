package com.misscut.utils;

import com.futureinteraction.utils.StringUtils;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class TimeParserUtils {
    @Getter
    private static final TimeParserUtils instance = new TimeParserUtils();

    @Getter
    private List<String> dateRegexes = new ArrayList<>();

    private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private TimeParserUtils() {

    }

    public void initDateRegexes(List<String> regexes) {
        dateRegexes.addAll(regexes);
    }

    public static Long parseDate(String text, List<String> dateRegexes) {
        if (StringUtils.isNullOrEmpty(text) || dateRegexes == null || dateRegexes.isEmpty())
            return null;

        long time = System.currentTimeMillis();
        List<Long> dates = new ArrayList<>();
        for (String dateRegex : dateRegexes) {
            Pattern datePattern = Pattern.compile(dateRegex);
            Matcher dateMatcher = datePattern.matcher(text);

            while (dateMatcher.find()) {
                String day = null;
                String month = null;
                String year = null;
                try {
                    int groupCount = dateMatcher.groupCount();
                    if (groupCount == 1) {
                        year = dateMatcher.group(1);
                        month = "01";
                        day = "01";
                    } else if (groupCount == 2) {
                        year = dateMatcher.group(1);
                        month = dateMatcher.group(2);
                        day = "01";
                        if (month.length() == 1)
                            month = "0" + month;
                    } else if (groupCount == 3) {
                        year = dateMatcher.group(1);
                        month = dateMatcher.group(2);
                        day = dateMatcher.group(3);
                        if (month.length() == 1)
                            month = "0" + month;
                        if (day.length() == 1)
                            day = "0" + day;
                    }
                } catch (Exception e) {
                    log.trace("no day in this text: {}", text);
                    day = "01";
                    month = "01";
                }

                String dateStr = year + '-' + month + '-' + day;
                try {
                    LocalDate localDate = LocalDate.parse(dateStr, formatter);
                    LocalDateTime dateTime = localDate.atStartOfDay(); // 转换为当天的午夜

                    ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
                    ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);

                    dates.add(zonedDateTime.toEpochSecond() * 1000);
                } catch (Exception e) {
                    log.error("failed to parse text {}, {}: {}", text, dateRegex, e.getMessage());
                }
            }
        }

        dates.sort(Collections.reverseOrder());

        log.trace("date parse cost {} ms", System.currentTimeMillis() - time);

        if (dates.isEmpty())
            return null;
        else
            return dates.get(0);
    }


}

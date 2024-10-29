package com.misscut.utils;

import com.futureinteraction.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Locale;

@Slf4j
public class TimeUtils {
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    public static DateTimeFormatter parseDateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm:ss]['T'HH:mm]['T'HH][ HH:mm:ss]");
    public static DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static DateTimeFormatter dateFormatter2 = DateTimeFormatter.ofPattern("yyyy年MM月dd日");
    private static SimpleDateFormat formatter = new SimpleDateFormat("EEE MMM dd HH:mm:ss Z yyyy", Locale.ENGLISH);


    public static long dateFormat(String str) {
        long timestamp = System.currentTimeMillis();
        try {
            Date date = formatter.parse(str);
            timestamp = date.getTime();
        } catch (ParseException e) {
            log.error("date format error: ", e);
        }

        return timestamp;
    }

    public static Long dateFormatTimestamp(String str) {
        if (StringUtils.isNullOrEmpty(str))
            return null;
        LocalDateTime dateTime = LocalDateTime.parse(str, DateTimeFormatter.ISO_DATE_TIME);
        String formattedDate = dateTime.format(dateTimeFormatter);
        LocalDateTime localDateTime = LocalDateTime.parse(formattedDate, dateTimeFormatter);
        // 转换为时间戳（秒）
        return localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;
    }

    public static Long dateFormatTimestampNoISO(String str) {
        if (StringUtils.isNullOrEmpty(str))
            return null;
        LocalDateTime localDateTime = LocalDateTime.parse(str, dateTimeFormatter);
        // 转换为时间戳（秒）
        return localDateTime.atZone(ZoneId.systemDefault()).toEpochSecond() * 1000;
    }


    public static long calDateBetween(String date1, String date2) {
        // 将字符串解析为LocalDate对象
        LocalDate startDate = LocalDate.parse(date1, dateFormatter);
        LocalDate endDate = LocalDate.parse(date2, dateFormatter);

        // 计算两个日期之间的天数差
        return ChronoUnit.DAYS.between(startDate, endDate);
    }


    public static String getTime() {
        LocalDateTime localDateTime = LocalDateTime.now();
        return localDateTime.format(dateTimeFormatter);
    }

    public static String getCurrentDate(Date date) {
        if (date == null)
            return null;
        Instant instant = date.toInstant();
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.format(dateFormatter);
    }

    public static String getCurrentDate2(Date date) {
        if (date == null)
            return null;
        Instant instant = date.toInstant();
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.format(dateFormatter2);
    }


    public static String getTime(Long time) {
        if (time == null)
            return null;

        Date date = new Date(time);
        Instant instant = date.toInstant();
        LocalDateTime localDateTime = instant.atZone(ZoneId.systemDefault()).toLocalDateTime();
        return localDateTime.format(dateTimeFormatter);
    }

    public static String getDate(int months, int weeks, int days) {
        LocalDate curDate = LocalDate.now();

        LocalDate localDate = curDate.plusMonths(months);
        localDate = localDate.plusWeeks(weeks);
        localDate = localDate.plusDays(days);

        LocalDateTime dateTime = localDate.atStartOfDay(); // 转换为当天的午夜

        ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
        ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);

        return zonedDateTime.format(dateTimeFormatter);
    }

    public static String getDate() {
        LocalDate localDate = LocalDate.now();
        LocalDateTime dateTime = localDate.atStartOfDay(); // 转换为当天的午夜

        ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
        ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);

        return zonedDateTime.format(dateTimeFormatter);
    }

    public static long getTimeInEpoch() {
        ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);

        return zonedDateTime.toEpochSecond() * 1000;
    }

    public static long getTimeInEpoch(String time) {
        try {
            LocalDateTime dateTime = LocalDateTime.parse(time, parseDateTimeFormatter);

            ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);

            return zonedDateTime.toEpochSecond() * 1000;
        }
        catch (Exception e) {
            LocalDateTime localDateTime = LocalDateTime.now(ZoneId.systemDefault());
            return localDateTime.toEpochSecond(ZoneOffset.UTC) * 1000;
        }
    }

    public static LocalDateTime getTime(String time) {
        try {
            LocalDateTime dateTime = LocalDateTime.parse(time, parseDateTimeFormatter);

            ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);

            return zonedDateTime.toLocalDateTime();
        }
        catch (Exception e) {
            LocalDateTime localDateTime = LocalDateTime.now(ZoneId.systemDefault());
            return localDateTime;
        }
    }

    public static long getTimeInMSecEpoch(String time) {
        ZoneId zoneId = ZoneId.systemDefault(); // 使用系统默认时区

        try {
            LocalDateTime dateTime = LocalDateTime.parse(time, parseDateTimeFormatter);
            ZonedDateTime zonedDateTime = dateTime.atZone(zoneId);
            return zonedDateTime.toEpochSecond() * 1000;
        }
        catch (Exception e) {
            LocalDateTime localDateTime = LocalDateTime.now(ZoneId.systemDefault());
            ZonedDateTime zonedDateTime = localDateTime.atZone(zoneId);
            return zonedDateTime.toEpochSecond() * 1000;
        }
    }

    public static int getSecondsOfDay() {
        LocalTime now = LocalTime.now();

        // 获取小时和分钟
        int hour = now.getHour();
        int minute = now.getMinute();
        int second = now.getSecond();

        return 3600*hour + 60*minute + second;
    }


    public static boolean isToday(String dateStr) {
        try {
            LocalDate date = LocalDate.parse(dateStr, dateFormatter);
            LocalDate today = LocalDate.now();
//            LocalDate today = LocalDate.parse("2024-05-30", dateFormatter);

            return date.equals(today);
        } catch (DateTimeParseException e) {
            return false;
        }
    }
}

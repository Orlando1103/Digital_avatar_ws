package com.misscut.utils;

import com.futureinteraction.utils.StringUtils;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;

public class FileMsProc {
    private static String DIR;
    private static final SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
    private static SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHHmmss");


    public static void setDIR(String DIR) {
        FileMsProc.DIR = DIR;
        FileMsPathProc.setDIR(DIR);
    }
    public static String getFilePath(long filetime, String uuid) {
        if (StringUtils.isNullOrEmpty(uuid))
            return null;

        StringBuilder sb = new StringBuilder();
        sb.append(getFileDirPath(DIR, filetime))
                .append("/")
                .append(uuid);

        return sb.toString();
    }

    public static String genFileId(long time, String uuid) {
        Date date = new Date(time);
        return sdf2.format(date) + "." + uuid;
    }

    public static String genFileId(long time) {
        Date date = new Date(time);
        return sdf2.format(date) + "." + UUID.randomUUID().toString();
    }

    /**
     * @param time: file create time
     * @return full dir path
     */
    static String getFileDirPath(String rootDir, long time) {
        String destDirName = rootDir + "/" + sdf.format(new Date(time));
        return destDirName;
    }

    public static byte[] readFile(String file) {
        try {
            File f = new File(file);
            byte[] buffer = new byte[(int) f.length()];
            FileInputStream is = new FileInputStream(file);
            is.read(buffer);
            is.close();
            return buffer;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }
}

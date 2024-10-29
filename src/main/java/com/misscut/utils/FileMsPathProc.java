package com.misscut.utils;

public class FileMsPathProc {
    private static String DIR;

    public static void setDIR(String DIR) {
        FileMsPathProc.DIR = DIR;
    }

    //"/Users/wwt/workspace/files-ms/files/"
    public static String getFilePath(String relativePath) {
        return DIR + "/" + relativePath;
    }
}

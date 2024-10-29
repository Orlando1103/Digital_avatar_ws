package com.misscut;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@Slf4j
public class Verinfo {
    /**
     * 获取版本信息
     * @return
     */
    private static void getAppVersion() {
        Properties properties = new Properties();
        try {
            properties.load(Verinfo.class.getClassLoader().getResourceAsStream("app.properties"));
            if (!properties.isEmpty()) {
                String ver = properties.getProperty("app.version");
                String buildTime = properties.getProperty("build.time");
                
                log.info("VER {}; BUILD TIME {}", ver, buildTime);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void dumpVersion() {
        try {
            InputStream is = Verinfo.class.getClassLoader().getResourceAsStream("banner.txt");
            assert is != null;
            byte[] bytes = is.readAllBytes();
            log.info(new String(bytes));
            
            getAppVersion();
        }
        catch (NullPointerException | IOException e) {
            e.printStackTrace();
        }
    }
}

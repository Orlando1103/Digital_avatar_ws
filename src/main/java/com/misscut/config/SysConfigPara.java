package com.misscut.config;

import com.misscut.model.ConstDef;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SysConfigPara {

    public DbVerticle db_verticle;
    public RestPara rest_verticle;
    public ScheduleVerticlePara schedule_verticle;
    public AlgoVerticlePara algo_verticle;

    public Map cluster;


    public RedisPara redis;
    public MetricsPara metrics;
    public static SysConfigPara conf;

    public static SysConfigPara load(String path) throws FileNotFoundException {
        try {
            log.trace("config:\n" + new String(new FileInputStream(path).readAllBytes()));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Yaml yaml = new Yaml();
        InputStream in = new FileInputStream(path);
        SysConfigPara config = yaml.loadAs(in, SysConfigPara.class);
        conf = config;
        return config;
    }

    @NoArgsConstructor
    @Data
    public static class MetricsPara {
        public String db_url;
        public String database;
    }


    @NoArgsConstructor
    @Data
    public static class MalApi {
        public String server_ip;
        public Integer server_port;
        public Integer gpt_type;
        public Integer sms_type;
        public String path;
    }


    @NoArgsConstructor
    @Data
    public static class WebsocketApi {
        public String server_ip;
        public Integer server_port;
        public String user_status_url;
        public String push_url;
    }


    @NoArgsConstructor
    @Data
    public static class CommonConfig {
        public Boolean test_signin;
    }


    @NoArgsConstructor
    @Data
    public static class ChatMalApi {
        public String server_ip;
        public Integer server_port;
        public String get_ai_reply;
        public String get_recommend_expert_uuid;
        public String handle_knowledge_accumulation;
        public String get_random_questions;
        public String get_chat_resource_info;
        public String get_chat_reply_basis;
        public String get_parent_reply_basis;
        public String get_chat_title;
        public String get_audio_path;
        public String get_file;
    }


    @NoArgsConstructor
    @Data
    public static class SmsConfig {
        public String sign_id;
        public String template_id;
    }


    @NoArgsConstructor
    @Data
    public static class DbVerticle {
        public String db_para;
        public Integer instance;
    }

    @NoArgsConstructor
    @Data
    public static class ScheduleVerticlePara {
        public int instance;
    }


    @NoArgsConstructor
    @Data
    public static class AlgoVerticlePara {
        public int instance;
    }


    @NoArgsConstructor
    @Data
    public static class Redis {
        public String type;
        public String[] hosts;
        public Integer max_pool_size;
        public Integer max_pool_waiting;
        public Integer max_per_connect_retries;
        public Integer max_reconnect_waiting_period;
    }
    
    @NoArgsConstructor
    @Data
    public static class RestPara {
        public String host;
        public int port;
        public int instance;
        public int worker_pool_size;
    }
    
    @NoArgsConstructor
    @Data
    public static class RedisPara {
        public String[] hosts;
        public String type;

        public int max_pool_size;
        public int max_pool_waiting;

        public int max_per_connect_retries;
        public int max_reconnect_waiting_period;
    }

    @NoArgsConstructor
    @Data
    public static class Jwt {
        public String algorithm;
        public String public_key;
        public Long max_token_time;
    }

    @NoArgsConstructor
    @Data
    public static class Media {
        public String file_dir;
        public String tmp_dir;
    }

    @NoArgsConstructor
    @Data
    public static class Vertx {
        public String file_cache_dir;
        public Integer worker_pool_size_cpus_ratio;
    }

    public List<String> time_regex;

    public Media media;

    public Vertx vertx;

    public MalApi mal_api;

    public WebsocketApi websocket_api;

    public ChatMalApi chat_mal_api;

    public SmsConfig sms_config;

    public Jwt jwt;

    public CommonConfig common_config;

}

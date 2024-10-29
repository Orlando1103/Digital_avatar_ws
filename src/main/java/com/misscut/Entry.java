package com.misscut;

import com.futureinteraction.utils.FileProc;
import com.futureinteraction.utils.FileUtils;
import com.futureinteraction.utils.metrics.MetricsInit;
import com.google.gson.Gson;
import com.hazelcast.config.Config;
import com.hazelcast.config.FileSystemYamlConfig;
import com.misscut.auth.JWTAuthProc;
import com.misscut.config.SysConfigPara;
import com.misscut.utils.FileMsProc;
import com.misscut.utils.TimeParserUtils;
import com.misscut.verticle.AlgoVerticle;
import com.misscut.verticle.DbVerticle;
import com.misscut.verticle.RestVerticle;
import com.misscut.verticle.ScheduleVertical;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBusOptions;
import io.vertx.core.file.FileSystemOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.servicediscovery.Record;
import io.vertx.servicediscovery.ServiceDiscovery;
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
public class Entry {
    private static ServiceDiscovery SRV_DISCOVERY;
    private static Vertx CLUSTER_VERTX;

    //key, id; value, name
    private static final Map<String, String> verticleMap = new HashMap<>();
    private static final Map<String, Record> recordMap = new HashMap<>();

    public static void main(String[] args) {

        //read version
        Verinfo.dumpVersion();

        //sys config init
        Options options = new Options();

        Option urlInput = new Option("c", "com/misscut/config", true, "config file path");
        urlInput.setRequired(true);
        options.addOption(urlInput);

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            log.error(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }

        String path = cmd.getOptionValue("c");

        Yaml yaml = new Yaml();
        try {
            //加载参数文件，解析参数
            SysConfigPara config = SysConfigPara.load(path);

            Gson gson = new Gson();


            int core = Runtime.getRuntime().availableProcessors();
            int ratio = 8;
            log.info("worker pool size: " + core * ratio);

            EventBusOptions eventBusOptions = new EventBusOptions();
            String localhost;
            if (config.cluster == null)
                localhost = InetAddress.getLocalHost().getHostAddress();
            else
                localhost = (String) config.cluster.getOrDefault("host", InetAddress.getLocalHost().getHostAddress());

            log.info("localhost {}", localhost);
            eventBusOptions.setHost(localhost);

            FileMsProc.setDIR(config.media.file_dir);
            FileProc.setDIR(config.media.tmp_dir);

            FileSystemOptions fileSystemOptions = new FileSystemOptions();
            fileSystemOptions.setFileCacheDir(config.vertx.file_cache_dir);

            VertxOptions vertxOptions = new VertxOptions();
            vertxOptions.setHAEnabled(true)
                    .setEventBusOptions(eventBusOptions)
                    .setWorkerPoolSize(core * ratio)
                    .setFileSystemOptions(fileSystemOptions);

            MicrometerMetricsOptions mo = config.metrics != null ? MetricsInit.initMetricsOptions(config.metrics.db_url, config.metrics.database) : null;
            if (mo != null) {
                mo.setJvmMetricsEnabled(true);
                vertxOptions.setMetricsOptions(mo);
            }

            JWTAuthProc.setJwtPara(
                    config.jwt.algorithm,
                    config.jwt.public_key,
                    config.jwt.max_token_time);

            VertxBuilder vertxBuilder = Vertx.builder().with(vertxOptions);

            Future<Vertx> vertxFuture;
            if (config.cluster != null) {
                Config hazelcastConfig;

                String configFile = (String) config.cluster.get("config");
                if (configFile != null)
                    hazelcastConfig = new FileSystemYamlConfig(configFile);
                else
                    hazelcastConfig = new Config();

                HazelcastClusterManager cm = new HazelcastClusterManager(hazelcastConfig);
                vertxBuilder.withClusterManager(cm);
                vertxFuture = vertxBuilder.buildClustered();
            }
            else {
                Promise<Vertx> promise = Promise.promise();
                promise.complete(vertxBuilder.build());
                vertxFuture = promise.future();
            }

            vertxFuture.onComplete(done -> {
                if (done.failed()) {
                    log.error("failed to init clustered vertx");
                } else {

                    int workerPoolSize = core * ratio;
                    log.info("worker pool size: {}", workerPoolSize);

                    CLUSTER_VERTX = done.result();
                    CLUSTER_VERTX.exceptionHandler(handler -> {
                        log.error(handler.getMessage());
                    });

                    SRV_DISCOVERY = ServiceDiscovery.create(CLUSTER_VERTX);

                    JWTAuthProc.init(CLUSTER_VERTX);

                    Function<Void, Future<Void>> restVerticle = v -> {
                        JsonObject verticlePara = new JsonObject();
                        verticlePara
                                .put("port", config.rest_verticle.port);

                        if (config.redis != null)
                            verticlePara.put("redis", gson.toJson(config.redis));

                        if (config.rest_verticle.host != null)
                            verticlePara.put("host", config.rest_verticle.host);

                        DeploymentOptions deploymentOptions = new DeploymentOptions()
                                .setConfig(verticlePara)
                                .setInstances(config.rest_verticle.instance)
                                .setWorkerPoolSize(workerPoolSize);

                        Promise<Void> promise = Promise.promise();
                        CLUSTER_VERTX.deployVerticle(RestVerticle.class, deploymentOptions, h -> {
                            if (h.failed()) {
                                log.error("failed to deploy rest server verticle: {}", h.cause().getMessage());
                                promise.fail(h.cause());
                            } else {
                                log.info("succeeded to deploy rest server verticle: {}", h.result());
                                verticleMap.put(h.result(), "rest server verticle");
                                promise.complete();
                            }
                        });


                        return promise.future();
                    };

                    Function<Void, Future<Void>> dbVerticle = v -> {
                        DeploymentOptions dbOptions = new DeploymentOptions()
                                .setInstances(config.db_verticle.instance)
                                .setWorker(true)
                                .setConfig(new JsonObject()
                                        .put("redis", gson.toJson(config.redis))
                                        .put("db", gson.toJson(config.db_verticle))
                                );

                        Promise<Void> subPromise = Promise.promise();
                        CLUSTER_VERTX.deployVerticle(DbVerticle.class, dbOptions, h -> {
                            if (h.failed()) {
                                log.error("failed to deploy db verticle: " + h.cause());
                                subPromise.fail("failed to deploy db verticle: " + h.cause());
                            } else {
                                log.info("succeeded to deploy db verticle: " + h.result());
                                verticleMap.put(h.result(), "db verticle");
                                subPromise.complete();
                            }
                        });

                        return subPromise.future();
                    };


                    Function<Void, Future<Void>> scheduleVerticle = v -> {
                        DeploymentOptions scheduleOptions = new DeploymentOptions()
                                .setInstances(config.schedule_verticle.instance)
                                .setWorker(true)
                                .setWorkerPoolName("schedule")
                                .setConfig(new JsonObject().put("redis", gson.toJson(config.redis)));

                        Promise<Void> subPromise = Promise.promise();
                        CLUSTER_VERTX.deployVerticle(ScheduleVertical.class, scheduleOptions, h -> {
                            if (h.failed()) {
                                log.error("failed to schedule verticle: " + h.cause());
                                subPromise.fail("failed to schedule verticle: " + h.cause());
                            } else {
                                log.info("succeeded to deploy schedule verticle: " + h.result());
                                subPromise.complete();
                            }
                        });
                        return subPromise.future();
                    };

                    Function<Void, Future<Void>> algoVerticle = v -> {
                        DeploymentOptions algoOptions = new DeploymentOptions()
                                .setInstances(config.algo_verticle.instance)
                                .setWorker(true)
                                .setConfig(new JsonObject().put("host", config.chat_mal_api.server_ip)
                                        .put("port", config.chat_mal_api.server_port));

                        Promise<Void> subPromise = Promise.promise();
                        CLUSTER_VERTX.deployVerticle(AlgoVerticle.class, algoOptions, h -> {
                            if (h.failed()) {
                                log.error("failed to deploy algo verticle: " + h.cause());
                                subPromise.fail("failed to deploy algo verticle: " + h.cause());
                            } else {
                                log.info("succeeded to deploy algo verticle: " + h.result());
                                verticleMap.put(h.result(), "algo verticle");
                                subPromise.complete();
                            }
                        });
                        return subPromise.future();
                    };


                    Promise<Void> startDeploy = Promise.promise();
                    startDeploy.complete();
                    startDeploy.future()
                            .compose(restVerticle)
                            .compose(dbVerticle)
                            .compose(scheduleVerticle)
                            .compose(algoVerticle)
                            .onFailure(failure -> {
                                log.error("failed to deploy virticles: {}", failure.getMessage());
                            }).onSuccess(success -> {
                                log.info("succeeded to deploy virticles");
                                TimeParserUtils.getInstance().initDateRegexes(config.time_regex);
                            });
                }
            });

            Runtime.getRuntime().addShutdownHook(new Thread() {
                public void run() {
                    System.out.println("to shutdown ...");
                    CountDownLatch latch = new CountDownLatch(verticleMap.size() + recordMap.size());

                    if (SRV_DISCOVERY != null) {
                        for (Record record : recordMap.values()) {
                            SRV_DISCOVERY.unpublish(record.getRegistration(), handler -> {
                                if (handler.failed()) {
                                    System.out.println(handler.cause().getMessage());
                                    log.error(handler.cause().getMessage());
                                } else {
                                    System.out.println("service unpublished: " + record.getName());
                                    log.info("service unpublished: " + record.getName());
                                }
                                latch.countDown();
                            });
                        }
                    }

                    if (CLUSTER_VERTX != null) {
                        for (String verticleId : verticleMap.keySet()) {
                            CLUSTER_VERTX.undeploy(verticleId, handler -> {
                                if (handler.failed()) {
                                    System.out.println(handler.cause().getMessage());
                                    log.error("failed to undeploy verticle: " + verticleMap.get(verticleId));
                                } else {
                                    System.out.println("verticle stopped: " + verticleMap.get(verticleId));
                                    log.info("succeeded to undeploy verticle" + verticleMap.get(verticleId));
                                }

                                latch.countDown();
                            });
                        }
                    }

                    try {
                        latch.await(5, TimeUnit.SECONDS);
                        System.out.println("shut down ...");
                    } catch (Exception ignored) {
                    }
                }
            });
        } catch (Exception e) {
            e.printStackTrace();
            log.error("failed to run: " + e.getMessage());
            System.exit(1);
        }
    }
}

package com.misscut.utils;

import com.futureinteraction.utils.RedisHolder;
import com.futureinteraction.utils.StringUtils;
import com.misscut.event.EventConst;
import com.misscut.model.ConstDef;
import com.misscut.resource.WebResource;
import com.sun.nio.file.SensitivityWatchEventModifier;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.WebClient;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/**
 * @Author WangWenTao
 * @Date 2024-08-06 17:16:49
 **/
@Slf4j
public class CommonUtils {

    private static final int SURNAME_PROBABILITY = 5;
    private static final Random RANDOM = new Random();
    //随机问题
    public static List RANDOM_QUESTION;

    private static final String FAMILY_ONE_NAME = "赵钱孙李周吴郑王冯陈褚卫蒋沈韩杨朱秦尤许何吕施张孔曹严华金魏陶姜戚谢邹喻水云苏潘葛奚范彭郎鲁韦昌马苗凤花方俞任" +
            "袁柳鲍史唐费岑薛雷贺倪汤滕殷罗毕郝邬安常乐于时傅卞齐康伍余元卜顾孟平黄和穆萧尹姚邵湛汪祁毛禹狄米贝明臧计成戴宋茅庞熊纪舒屈项祝董粱杜阮" +
            "席季麻强贾路娄危江童颜郭梅盛林刁钟徐邱骆高夏蔡田胡凌霍万柯卢莫房缪干解应宗丁宣邓郁单杭洪包诸左石崔吉龚程邢滑裴陆荣翁荀羊甄家封芮储靳邴" +
            "松井富乌焦巴弓牧隗山谷车侯伊宁仇祖武符刘景詹束龙叶幸司韶黎乔苍双闻莘劳逄姬冉宰桂牛寿通边燕冀尚农温庄晏瞿茹习鱼容向古戈终居衡步都耿满弘" +
            "国文东殴沃曾关红游盖益桓公晋楚闫";

    private static final String FAMILY_TWO_NAME = "欧阳太史端木上官司马东方独孤南宫万俟闻人夏侯诸葛尉迟公羊赫连澹台皇甫宗政濮阳公冶太叔申屠公孙慕容仲孙钟离长孙宇" +
            "文司徒鲜于司空闾丘子车亓官司寇巫马公西颛孙壤驷公良漆雕乐正宰父谷梁拓跋夹谷轩辕令狐段干百里呼延东郭南门羊舌微生公户公玉公仪梁丘公仲公上" +
            "公门公山公坚左丘公伯西门公祖第五公乘贯丘公皙南荣东里东宫仲长子书子桑即墨达奚褚师吴铭";


    public static int randomInt(int maxNum) {
        return RANDOM.nextInt(maxNum);
    }

    public static String getRandomBoyName() {
        String boyName = "伟刚勇毅俊峰强军平保东文辉力明永健世广志义兴良海山仁波宁贵福生龙元全国胜学祥才发武新利清飞彬富顺信子杰涛昌成康星光天达" +
                "安岩中茂进林有坚和彪博诚先敬震振壮会思群豪心邦承乐绍功松善厚庆磊民友裕河哲江超浩亮政谦亨奇固之轮翰朗伯宏言若鸣朋斌梁栋维启克伦翔旭鹏泽" +
                "晨辰士以建家致树炎德行时泰盛雄琛钧冠策腾楠榕风航弘";
        int bodNameIndexOne = randomInt(boyName.length());
        int bodNameIndexTwo = randomInt(boyName.length());
        if (randomInt(100) > SURNAME_PROBABILITY) {
            int familyOneNameIndex = randomInt(FAMILY_ONE_NAME.length());
            return boyName.substring(bodNameIndexOne, bodNameIndexOne + 1) + FAMILY_ONE_NAME.substring(familyOneNameIndex, familyOneNameIndex + 1) +

                    boyName.substring(bodNameIndexTwo, bodNameIndexTwo + 1);
        } else {
            int familyTwoNameIndex = randomInt(FAMILY_TWO_NAME.length());
            familyTwoNameIndex = familyTwoNameIndex % 2 == 0 ? familyTwoNameIndex : familyTwoNameIndex - 1;
            return
                    boyName.substring(bodNameIndexOne, bodNameIndexOne + 1) +
                            FAMILY_TWO_NAME.substring(familyTwoNameIndex, familyTwoNameIndex + 2) +
                            boyName.substring(bodNameIndexTwo, bodNameIndexTwo + 1);
        }
    }


    @Deprecated
    public static List<String> getRandomQuestion(int cnt) {
        List<String> shuffledList = new ArrayList<>(RANDOM_QUESTION);
        Collections.shuffle(shuffledList);

        return new ArrayList<>(shuffledList.subList(0, cnt));
    }


    public static void onChangeFile() throws IOException {
        Path path = Paths.get("config-file");
        WatchService watchService = FileSystems.getDefault().newWatchService();

        path.register(watchService,
                new WatchEvent.Kind[]{
                        StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_CREATE,
                        StandardWatchEventKinds.ENTRY_DELETE
                },
                SensitivityWatchEventModifier.HIGH);

        Thread watchThread = new Thread(() -> {
            log.info("start to listening file changed");
            while (true) {
                log.info("reset key ready to listen...");
                WatchKey key;
                try {
                    key = watchService.take();
                } catch (InterruptedException e) {
                    return;
                }
                for (WatchEvent<?> event : key.pollEvents()) {
                    if (event.kind() == StandardWatchEventKinds.ENTRY_MODIFY) {
                        Path changedFile = (Path) event.context();
                        log.info("the changed path: {}", changedFile);
                        // 读取文件内容并更新到本地缓存
                        try {
                            switch (changedFile.toString()) {
                                case "random_question.json" -> {
                                    String content = new String(Files.readAllBytes(Path.of("config-file/" + changedFile)));
                                    log.info("the content: {}", content);
                                    CommonUtils.RANDOM_QUESTION = new JsonArray(content).getList();
                                }
                                default -> log.info("the file: {} but no update", changedFile);
                            }

                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                }
                if (!key.reset()) {
                    break;
                }
            }
        });

        watchThread.start();
    }


    public static JsonObject genPushData(String cmd, JsonArray data, JsonArray ids, int receiverType, String batchId) {
        return new JsonObject()
                .put("data", new JsonObject().put("type", 3).put("cmd", cmd).put("data", data).put("receiver_type", receiverType))
                .put("ids", ids)
                .put("batch_id", batchId);
    }


    public static List<String> updateParaList(List<String> paraList, String paraKey, Object paraValue, String opt) {
        List<String> resList = new ArrayList<>();
        if (CollectionUtils.isEmpty(paraList)) {
            resList.add("[" + paraKey + opt + paraValue + "]");
        } else {
            for (int i = 0; i < paraList.size(); i++) {
                String para = paraList.get(i);
                if (StringUtils.isNullOrEmpty(para)) {
                    para = "[" + paraKey + opt + paraValue + "]";
                } else {
                    para = para + "[" + paraKey + opt + paraValue + "]";
                }
                resList.add(para);
            }
        }

        return resList;
    }


    public static boolean isValidPhoneNumber(String phoneNumber, Boolean isTest) {
        String regex;
        if (isTest) {
            regex = "^1000\\d{7}$";
        } else {
            regex = "^(13|14|15|17|16|18|19)\\d{9}$";
        }

        return phoneNumber.matches(regex);
    }


    public static void checkChat2GenScoreMsg(Vertx vertx, RedisHolder redis) {
        JsonObject para = new JsonObject().put("time_diff", 1);
        DeliveryOptions d1 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.CHAT.ACTIONS.QUERY_CHAT_BY_TIME_DIFF);
        List<Future<Message<JsonObject>>> futures = new ArrayList<>();
        Promise<Pair<JsonArray, List<Future<Message<JsonObject>>>>> promise = Promise.promise();
        vertx.eventBus().<JsonArray>request(EventConst.CHAT.ID, para, d1).compose(success -> {
            JsonArray body = success.body();
            JsonArray toGenerateScoreMsgChats = new JsonArray();
            for (int i = 0; i < body.size(); i++) {
                JsonObject chat = body.getJsonObject(i);
                String chatUuid = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                JsonArray deletedUsers = chat.getJsonArray(ConstDef.CHAT_INFO_DATA_KEYS.DELETED_USERS);
                String senderId = chat.getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID);
                if (deletedUsers != null && deletedUsers.contains(senderId))
                    continue;
                toGenerateScoreMsgChats.add(chat);
                DeliveryOptions d2 = new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.QUERY_LATEST_MSG_BY_CHAT_UUID);
                futures.add(vertx.eventBus().request(EventConst.MESSAGE.ID, new JsonObject().put("chat_uuid", chatUuid), d2));
            }
            Future.join(futures).onComplete(done -> promise.complete(Pair.of(toGenerateScoreMsgChats, futures)));

            return promise.future();
        }).onSuccess(success -> {
            JsonArray chats = success.getLeft();
            log.trace("the chats: {}", chats.encodePrettily());
            JsonArray scoreMsgList = new JsonArray();
            for (int i = 0; i < futures.size(); i++) {
                Future<Message<JsonObject>> future = futures.get(i);
                if (future.succeeded()) {
                    JsonObject latestMsg = future.result().body();
                    log.trace("the latest msg: {}", latestMsg.encode());
                    if (latestMsg.isEmpty())
                        continue;
                    Integer messageType = latestMsg.getInteger(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE);
                    if (messageType != ConstDef.MSG_TYPE.USER_SCORE && messageType != ConstDef.MSG_TYPE.CARD) {
                        String chatUuid = chats.getJsonObject(i).getString(ConstDef.CHAT_INFO_DATA_KEYS.CHAT_UUID);
                        log.debug("to generate score msg: {}", chatUuid);
                        JsonObject message = new JsonObject()
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_UUID, UUID.randomUUID() + "-msg")
                                .put(ConstDef.MESSAGE_DATA_KEYS.CHAT_UUID, chatUuid)
                                .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_IDENTITY, ConstDef.USER_IDENTITY.BOT)
                                .put(ConstDef.MESSAGE_DATA_KEYS.SENDER_ID, "system")
                                .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_ID, chats.getJsonObject(i).getString(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_ID))
                                .put(ConstDef.MESSAGE_DATA_KEYS.RECEIVER_IDENTITY, chats.getJsonObject(i).getInteger(ConstDef.CHAT_INFO_DATA_KEYS.SENDER_IDENTITY))
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_TYPE, ConstDef.MSG_TYPE.USER_SCORE)
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_STATUS, ConstDef.MSG_STATUS.PENDING)
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CATEGORY, ConstDef.MSG_CATEGORY.TEXT)
                                .put(ConstDef.MESSAGE_DATA_KEYS.MESSAGE_CONTENT, "请您对本次咨询进行评价")
                                .put(ConstDef.MESSAGE_DATA_KEYS.MACHINE_SCORE, null);

                        scoreMsgList.add(message);
                    }
                }
            }
            log.trace("the score list: {}", scoreMsgList);
            if (scoreMsgList.isEmpty())
                return;
            JsonObject addPara = new JsonObject().put("msgList", scoreMsgList);
            vertx.eventBus().<JsonObject>request(EventConst.MESSAGE.ID, addPara, new DeliveryOptions().addHeader(EventConst.ACTION, EventConst.MESSAGE.ACTIONS.ADD_BATCH))
                    .onSuccess(v -> {
                        log.debug("batch add score msg succeed");
                        WebClient client = WebClient.create(vertx);
                        PushUtils.pushAddedMsg(vertx, client, redis, scoreMsgList, false);
                    })
                    .onFailure(failure -> {
                        log.error("add score msg error: {}", failure.getMessage());
                    });
        }).onFailure(failure -> {
            log.error("checkChat2GenScoreMsg error: {}", failure.getMessage());
        });
    }

}

package com.misscut.model;

/**
 * @Author WangWenTao
 * @Date 2024-03-13 13:55:00
 **/
public interface ConstDef {

    interface JWT {
        String TIME = "time";
        String DEV_SERIAL = "dev-serial";
        String LOGIN_TYPE_FLAG = "login-type-flag";
    }


    interface USER_BASIS_DATA_KEYS {
        String ID = "id";
        String USER_UID = "user_uid";
        String USER_IDENTITY = "user_identity";
        String USER_PHONE = "user_phone";
        String USER_NAME = "user_name";
        String USER_SEX = "user_sex";
        String USER_AGE = "user_age";
        String USER_AVATAR = "user_avatar";
        String USER_REG_TIME = "user_reg_time";
    }


    interface PARENT_INFO_DATA_KEYS {
        String ID = "id";
        String USER_ID = "user_id";
        String PASSWORD = "password";
        String EDUCATION_LEVEL = "education_level";
        String EMPLOYMENT = "employment";
        String TAG = "tag";
    }


    interface CHILD_INFO_DATA_KEYS {
        String ID = "id";
        String CHILD_ID = "child_id";
        String PARENT_ID = "parent_id";
        String CHILD_NAME = "child_name";
        String CHILD_SEX = "child_sex";
        String CHILD_AGE = "child_age";
        String MENTAL_HEALTH = "mental_health";
        String EDUCATION_LEVEL = "education_level";
        String ACADEMIC_RECORD = "academic_record";
    }


    interface FAMILY_INFO_DATA_KEYS {
        String ID = "id";
        String PARENT_ID = "parent_id";
        String FAMILY_TYPE = "family_type";
        String ACCOMPANY = "accompany";
        String COMMUNICATE = "communicate";
        String RELATIONSHIP = "relationship";
    }


    interface EXPERT_INFO_DATA_KEYS {
        String ID = "id";
        String USER_ID = "user_id";
        String PASSWORD = "password";
        String FIELD = "field";
        String LEVEL = "level";
        String SENIORITY = "seniority";
    }


    interface CHAT_INFO_DATA_KEYS {
        String ID = "id";
        String CHAT_UUID = "chat_uuid";
        String SENDER_IDENTITY = "sender_identity";
        String RECEIVER_IDENTITY = "receiver_identity";
        String SENDER_ID = "sender_id";
        String RECEIVER_ID = "receiver_id";
        String CHAT_KEYWORD = "chat_keyword";
//        String CHAT_KNOWLEDGE_BASE_ID = "chat_knowledge_base_id";
        String CHAT_TITLE = "chat_title";
        String CHAT_STATUS = "chat_status";
        String CHAT_ROUNDS = "chat_rounds";
        String FAVORITE_STATUS = "favorite_status";
        String FAVORITE_TIME = "favorite_time";
        String CREATE_TIME = "create_time";
        String LATEST_MESSAGE_TIME = "latest_message_time";
        String LATEST_READ_TIME = "latest_read_time";
        String DELETED_USERS = "deleted_users";

//        String IS_DELETED = "is_deleted";
//        String DELETED_AT = "deleted_at";
    }


    interface REPLY_BASIS_DATA_KEYS {
        String ID = "id";
        String OWNER_ID = "owner_id";
        String SUPPLYING = "supplying";
        String PROFILE = "profile";
        String REPLY_STRATEGY = "reply_strategy";
        String EVENT_SUMMARY = "event_summary";
    }


    interface ORGANIZATION_INFO_DATA_KEYS {
        String ID = "id";
        String ORGANIZATION_NAME = "organization_name";
        String INVITE_CODE = "invite_code";
    }


    interface ORGANIZATION_MEMBER_INFO_DATA_KEYS {
        String ID = "id";
        String ORGANIZATION_ID = "organization_id";
        String MEMBER_ID = "member_id";
        String MEMBER_IDENTITY = "member_identity";
    }


    interface MESSAGE_DATA_KEYS {
        String ID = "id";
        String MESSAGE_UUID = "message_uuid";
        String CHAT_UUID = "chat_uuid";
        String MESSAGE_TYPE = "message_type";
        String MESSAGE_CATEGORY = "message_category";
        String MESSAGE_CONTENT = "message_content";
        String SENDER_IDENTITY = "sender_identity";
        String RECEIVER_IDENTITY = "receiver_identity";
        String SENDER_ID = "sender_id";
        String RECEIVER_ID = "receiver_id";
        String MACHINE_SCORE = "machine_score";
        String MESSAGE_STATUS = "message_status";
        String CREATE_TIME = "create_time";
        String DELETED_USERS = "deleted_users";

//        String IS_DELETED = "is_deleted";
//        String DELETED_AT = "deleted_at";
    }


    interface MESSAGE_INFO_DATA_KEYS {
        String ID = "id";
        String MESSAGE_UUID = "message_uuid";
        String QUOTE_CONTENT = "quote_content";
        String QUOTE_MESSAGE_UUID = "quote_message_uuid";
        String AUDIO_PATH = "audio_path";
        String AUDIO_DURATION = "audio_duration";
        String CARD_TYPE = "card_type";
        String CHAT_RESOURCE_ID = "chat_resource_id";
        String BUTTON_TYPE = "button_type";
        String EXPERT_SCORE = "expert_score";
        String EXPERT_FEEDBACK = "expert_feedback";
        String EXPERT_REVISION = "expert_revision";
        String PARENT_LIKE_STATUS = "parent_like_status";
        String PARENT_SCORE = "parent_score";
        String PARENT_FEEDBACK = "parent_feedback";
//        String IS_DELETED = "is_deleted";
//        String DELETED_AT = "deleted_at";
    }


    interface CHAT_RESOURCE {
        String ID = "id";
        String CHAT_UUID = "chat_uuid";
        String TITLE = "title";
        String SOURCE = "source";
        String RESOURCE_TYPE = "resource_type";
        String RESOURCE_URL = "resource_url";
        String RESOURCE_PATH = "resource_path";
        String CREATE_TIME = "create_time";
        String IS_DELETED = "is_deleted";
        String DELETED_AT = "deleted_at";
    }


//    interface CHAT_KNOWLEDGE_INFO_DATA_KEYS {
//        String ID = "id";
//        String TITLE = "title";
//        String SOURCE = "source";
//        String RESOURCE_TYPE = "resource_type";
//        String RESOURCE_URL = "resource_url";
//        String RESOURCE_PATH = "resource_path";
//    }


//    interface CHAT_KNOWLEDGE_BASIS_DATA_KEYS {
//        String ID = "id";
//        String CHAT_UUID = "chat_uuid";
//        String PARENT_ID = "parent_id";
//        String KNOWLEDGE_STATUS = "knowledge_status";
//        String INFO_SCORE = "info_score";
//        String PARENT_SCORE = "parent_score";
//        String RETRIVAL_SCORE = "retrival_score";
//        String POLICY_STATE = "policy_state";
//        String IS_DELETED = "is_deleted";
//        String DELETED_AT = "deleted_at";
//    }


    interface CHAT_USED_KNOWLEDGE_INFO_DATA_KEYS {
        String ID = "id";
        String CHAT_UUID = "chat_uuid";
        String KNOWLEDGE_UUID = "knowledge_uuid";
        String FREQUENCY = "frequency";
//        String IS_DELETED = "is_deleted";
//        String DELETED_AT = "deleted_at";
    }


    interface LOGIC_DATA_KEYS {
        String ID = "id";
        String LOGIC_KEY_ID = "logic_key_id";
        String EMOTIONAL = "emotional";
        String FOCUS = "focus";
        String LOGIC_TEXT = "logic_text";
    }


    interface LOGIC_KEY_DATA_KEYS {
        String ID = "id";
        String KEY_TEXT = "key_text";
    }


    interface USER_IDENTITY {
        int PARENT = 0;
        int EXPERT = 1;
        int BOT = 2;
    }


    interface CHAT_STATUS {
        int AI = 0;
        int EXPERT_TO_INTERVENE = 1;
        int EXPERT_ALREADY_INTERVENED = 2;
    }


    interface MSG_STATUS {
        String PENDING = "pending";
        String RECEIVED = "received";
        String READ = "read";
        String PROCESSED = "processed";
    }

    interface MSG_CATEGORY {
        int TEXT = 0;
        int AUDIO = 1;
    }


    interface MSG_TYPE {
        int SYSTEM = 0;
        int TEXT = 1;
        int IMG = 2;
        int AUDIO = 3;
        int AUDIO_CALL = 4;
        int CARD = 5;
        int BUTTON = 6;
        int USER_SCORE = 7;
    }


    interface REPLY_CMD {
        String MSG_ACK = "MSG_ACK";
        String CREATE_MSG_REPLY = "CREATE_MSG_REPLY";
        String MSG_EDIT = "MSG_EDIT";
        String ADD_CHAT = "ADD_CHAT";
        String UPDATE_CHAT = "UPDATE_CHAT";
        String AI_ASSISTANT_RESP = "AI_ASSISTANT_RESP";
    }

    interface CHAT_IS_FAVORITE {
        int NO_COLLECT = 0;
        int COLLECT = 1;
    }

    interface CHAT_RESOURCE_TYPE {
        int RECOMMEND_EXPERT = 1;
        int VIDEO = 2;
        int AUDIO = 3;
        int WEB = 4;
        int TEXT_FILE = 5;
    }
}

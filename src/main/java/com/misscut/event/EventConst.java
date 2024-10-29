package com.misscut.event;


/**
 * EventBus相关常量类
 */
public interface EventConst {

    String ACTION = "action";

    interface ALGO_API {
        String ID = "algo-api";
        interface KEYS {
            String PARA = "para";
            String URL = "url";
            String STREAM = "stream";
        }
    }


    interface USER {
        String ID = "USER-DATA";

        interface KEYS {
            String QUERY_PARA = "query-para";
        }

        interface ACTIONS {
            String ADD = "add";
            String UPDATE = "update";
            String UPDATE_CHILD = "update-child";
            String DELETE_CHILD = "delete-child";
            String UPDATE_FAMILY = "update-family";
            String REMOVE = "remove";
            String QUERY_BY_PAGE = "query-by-page";
            String QUERY_BY_ID = "query-by-id";
            String QUERY_ALL_EXPERTS = "query-all-experts";
            String QUERY_BY_USER_UID = "query-by-user-uid";
            String QUERY_PARENT_INFO_BY_USER_ID = "query-parent-info-by-user-id";
            String QUERY_EXPERT_INFO_BY_USER_ID = "query-expert-info-by-user-id";
            String QUERY_FAMILY_INFO_BY_PARENT_ID = "query-family-info-by-parent-id";
            String QUERY_CHILD_INFO_BY_PARENT_ID = "query-child-info-by-parent-id";

            String QUERY_ORG_BY_INVITE_CODE = "query-org-by-invite-code";
            String QUERY_ORG_BY_ID = "query-org-by-id";
            String ADD_ORG_MEMBER = "add-org-member";
            String QUERY_ORG_MEMBER_BY_MEMBER_ID = "query-org-member-by-member-id";
            String QUERY_ORG_MEMBER_BY_ORG_ID = "query-org-member-by-org-id";
            String QUERY_ORG_MEMBER_BY_PARA = "query-org-member-by-para";
            String DELETE_ORG = "delete-org";

        }
    }


    interface CHAT {
        String ID = "CHAT";

        interface KEYS {
            String QUERY_PARA = "query-para";
        }

        interface ACTIONS {
            String ADD = "add";
            String UPDATE = "update";
            String DELETE = "delete";
            String QUERY_BY_PAGE = "query-by-page";
            String QUERY_BY_ID = "query-by-id";
            String QUERY_BY_CHAT_UUID = "query-chat-uuid";
            String QUERY_ALL = "query-all";
            String QUERY_ALL_BY_PARA = "query-all-by-para";
            String QUERY_RESOURCE = "query-resource";
            String QUERY_RESOURCE_BY_ID = "query-resource-by-id";
            String DELETE_RESOURCE = "delete-resource";
            String QUERY_CHAT_BY_TIME_DIFF = "query-time-diff-chat";
            String ADD_BATCH_CHAT_RESOURCE = "add-batch-chat-resource";
        }
    }



    interface MESSAGE {
        String ID = "MESSAGE";

        interface KEYS {
            String QUERY_PARA = "query-para";
        }

        interface ACTIONS {
            String ADD = "add";
            String ADD_BATCH = "add-batch";
            String UPDATE = "update";
            String DELETE = "delete";
            String QUERY_BY_PAGE = "query-by-page";
            String QUERY_BY_ID = "query-by-id";
            String QUERY_ALL_BY_CHAT_UUID = "query-all-by-chat-uuid";
            String QUERY_ALL_BY_PARA = "query-all-by-para";
            String QUERY_ONE = "query-one";
            String QUERY_MSG_INFO_BY_MSG_UUID = "query-msg-info-by-msg-uuid";
//            String QUERY_MSG_RESOURCE_BY_MSG_UUID = "query-msg-resource-by-msg-uuid";
            String UPDATE_MSG_INFO = "update-msg-info";
            String QUERY_LATEST_MSG_BY_CHAT_UUID_AND_SENDER = "query-latest-msg-by-chat-uuid-and-sender";
            String QUERY_LATEST_MSG_BY_CHAT_UUID = "query-latest-msg-by-chat-uuid";
            String QUERY_CHAT_ROUNDS = "query-chat-rounds";
        }
    }


    interface COMMON {
        String ID = "COMMON";

        interface KEYS {
            String QUERY_PARA = "query-para";
        }

        interface REPLY_BASIS {
            interface ACTIONS {
                String ADD_REPLY_BASIS = "add-reply-basis";
                String UPDATE = "update";
                String DELETE = "delete";
                String QUERY_BY_PAGE = "query-by-page";
                String QUERY_BY_ID = "query-by-id";
                String QUERY_BY_OWNER_ID = "query-by-owner-id";
                String QUERY_REPLY_BASIS_BY_OWNER_IDS = "query-reply-basis-by-owner-ids";
            }
        }

        interface LOGIC_KEY {
            interface ACTIONS {
                String ADD_LOGIC_KEY = "add-logic-key";
                String UPDATE = "update";
                String DELETE = "delete";
                String QUERY_BY_PAGE = "query-by-page";
                String QUERY_BY_ID = "query-by-id";
                String QUERY_ALL = "query-all";
            }
        }

        interface LOGIC {
            interface ACTIONS {
                String ADD_LOGIC = "add-logic";
                String UPDATE = "update";
                String DELETE = "delete";
                String QUERY_BY_PAGE = "query-by-page";
                String QUERY_BY_ID = "query-by-id";
                String QUERY_ALL = "query-all";
                String QUERY_BY_LOGIC_KEY_ID = "query-by-logic-key-id";
            }
        }

        interface CHAT_USED_KNOWLEDGE {
            interface ACTIONS {
                String ADD = "add";
                String UPDATE = "update";
                String DELETE = "delete";
                String QUERY_BY_PAGE = "query-by-page";
                String QUERY_BY_ID = "query-by-id";
                String QUERY_ALL = "query-all";
                String ADD_BATCH = "add-batch";
                String QUERY_TOP_N_BY_CHAT_UUID = "query-top-n-by-chat-uuid";
            }
        }


    }




    interface FILES {
        String ID = "files";

        interface REQ {
            interface ACTIONS {
                String ADD_ONE = "add-one";
                String ADD_OR_UPDATE_ONE = "add-or-update-one";
                String ADD_LIST_IN_OVERWRITE_BY_BASE64 = "add-list-in-overwrite-by-base64";
                String ADD_OR_UPDATE_ONE_BY_BASE64 = "add-or-update-one-by-base64";
                String ADD_OR_UPDATE_ONE_BY_BUFFER = "add-or-update-one-by-buffer";
                String ADD_OR_UPDATE_ONE_BY_UUID = "add-or-update-one-by-uuid";
                String GET_FILE_BY_UUID = "get-file-by-uuid";
                String ADD_ONE_TMP = "add-one-tmp";
                String BIND = "bind";
                String UPDATE_BIND_BY_IDS = "update-bind-by-ids";
                String UPDATE_BIND_BY_OBJ = "update-bind-by-obj";
                String BIND_OSS = "bind-oss";
                String GET_ONE = "get-one";
                String GET_ONE_TMP = "get-one-tmp";
                String GET_ONE_RELATED_FILE = "get-one-related-file";
                String GET_RELATED_FILES = "get-related-files";
                String GET_ONE_RELATED_FILE_IN_BASE64 = "get-one-related-file-in-base64";
                String DELETE_ONE = "delete-one";
                String DELETE_ONE_TMP = "delete-one-tmp";
                String DELETE_BY_OBJ_ID = "delete-by-obj-id";
            }

            interface KEYS {
                String ID = "id";
                String IDS = "ids";
                String DATA = "data";
                String BIND_PARA = "bind-para";
                String OBJ = "obj";
                String OBJ_ID = "obj-id";
                String NEW_OBJ = "new-obj";
                String NEW_OBJ_ID = "new-obj-id";
                String OBJ_ID_LIST = "obj-id-list";
                String FILE_RELATIVE_PATH = "file-relative-path";
                String FILE_BUFFER = "file-buffer";
                String TIME = "time";
                String UUID = "uuid";
                String NAME = "name";
                String MD5 = "md5";
                String OSS_CODE = "oss-code";
                String USAGE = "usage";
                String BASE64_DATA = "base64-data";
            }
        }

        interface REPLY {
            interface COMMON_KEYS {
                String ID = "id";
                String SUM = "sum";
                String RESULT = "result";
                String FILE_NAME = "file-name";
                String FILE_PATH = "file-path";
                String FILE_MD5 = "file-md5";
            }
        }
    }

}
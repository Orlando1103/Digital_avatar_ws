package com.misscut.utils;

import com.futureinteraction.utils.StringUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author WangWenTao
 * @Date 2024-01-16 15:48:40
 **/
public class RequestUtils {
    
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
}

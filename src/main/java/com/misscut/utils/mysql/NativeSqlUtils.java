package com.misscut.utils.mysql;

import com.futureinteraction.utils.StringUtils;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class NativeSqlUtils {

    private static int MAX_SIZE = 10000;
    private static String PARA_SPLITTER = ",";

    public static String WHERE = " WHERE ";
    public static String AND = " AND ";
    public static String OR = " OR ";

    private interface BIT_OPT {
        String AND = "and";
        String OR = "or";
        String XOR = "xor";
    }

    private interface LIKE_OPT {
        String L_LIKE = "l_like";
        String R_LIKE = "r_like";
        String B_LIKE = "b_like";
    }

    public static String IN_EXP = " IN (?) ";
    public static String WHERE_11_EXP = " WHERE 1=1 ";

    //need para
    public static String genInExpSql(String sql, int size) {
        if (size == 0)
            return sql + " IN (NULL)";

        StringBuilder sb = new StringBuilder(sql);
        sb.append(" IN ");
        sb.append("(");
        for (int i = 0; i < size; i++) {
            sb.append("?,");
        }

        sb.delete(sb.length() - 1, sb.length());
        sb.append(")");
        return sb.toString();
    }

    public static String getCountConditionSql(String tablename, boolean needWherePrefix, List<String> queryParaList) {

        StringBuilder sql = new StringBuilder();

        if (queryParaList != null && !queryParaList.isEmpty()){
            if (needWherePrefix)
                sql.append(WHERE);
            else
                sql.append(AND);


            for (String queryPara : queryParaList) {
                String whereSql = NativeSqlUtils.querySql(tablename, queryPara);
                if (!whereSql.isEmpty())
                    sql.append("(").append(whereSql).append(")").append(OR);
            }

            if (!queryParaList.isEmpty()) {
                int lastIndex = sql.lastIndexOf(OR);
                sql.delete(lastIndex, sql.length());
            }
        }

        return sql.toString();
    }

    public static String getConditionSql(String tablename, boolean needWherePrefix, List<String> queryParaList) {

        StringBuilder sql = new StringBuilder();

        if (queryParaList != null && !queryParaList.isEmpty()) {
            if (needWherePrefix)
                sql.append(WHERE);
            else
                sql.append(AND);

            for (String queryPara : queryParaList) {
                String whereSql = NativeSqlUtils.querySql(tablename, queryPara);
                sql.append("(").append(whereSql).append(")").append(OR);
            }

            if (!queryParaList.isEmpty()) {
                int lastIndex = sql.lastIndexOf(OR);
                sql.delete(lastIndex, sql.length());
            }
        }

        return sql.toString();
    }

    public static String getConditionSql(String tablename, boolean needWherePrefix, List<String> queryParaList, String sortPara) {

        StringBuilder sql = new StringBuilder();

        if (queryParaList != null && !queryParaList.isEmpty()) {
            if (needWherePrefix)
                sql.append(WHERE);
            else
                sql.append(AND);

            for (String queryPara : queryParaList) {
                String whereSql = NativeSqlUtils.querySql(tablename, queryPara);
                sql.append("(").append(whereSql).append(")").append(OR);
            }

            if (!queryParaList.isEmpty()) {
                int lastIndex = sql.lastIndexOf(OR);
                sql.delete(lastIndex, sql.length());
            }
        }

        String sortSql = NativeSqlUtils.getSortSql(tablename, sortPara);
        if (!sortSql.isEmpty())
            sql.append(" ").append(sortSql);

        return sql.toString();
    }

    public static String getConditionSql(String tablename, boolean needWherePrefix, List<String> queryParaList, String sortPara, int limit, int offset) {

        StringBuilder sql = new StringBuilder();

        if (queryParaList != null && !queryParaList.isEmpty()) {
            if (needWherePrefix)
                sql.append(WHERE);
            else
                sql.append(AND);

            for (String queryPara : queryParaList) {
                String whereSql = NativeSqlUtils.querySql(tablename, queryPara);
                sql.append("(").append(whereSql).append(")").append(OR);
            }

            if (!queryParaList.isEmpty()) {
                int lastIndex = sql.lastIndexOf(OR);
                sql.delete(lastIndex, sql.length());
            }
        }

        String sortSql = NativeSqlUtils.getSortSql(tablename, sortPara);
        if (!sortSql.isEmpty())
            sql.append(" ").append(sortSql);

        String limitSql = NativeSqlUtils.getLimitSql(limit, offset);
        if (!limitSql.isEmpty())
            sql.append(" ").append(limitSql);

        return sql.toString();
    }

    /**
     * @param tablename: table name
     * @param sort:      [col:asc], ... ,[col:desc]
     * @return
     */
    public static String getSortSql(String tablename, String sort) {
        if (sort == null)
            return "";

        StringBuilder sql = new StringBuilder();
        List<String> paras = splitParas(sort);

        for (String para : paras) {
            if (para.trim().isEmpty())
                continue;

            int pos1 = para.indexOf('[');
            int pos2 = para.indexOf(']');

            if (pos1 < 0 || pos2 < 0 || pos1 > pos2)
                continue;

            int pos3 = para.indexOf('~');
            if (pos3 > pos1+1) {
                String table = para.substring(pos1 + 1, pos3);

                String[] truePara = para.substring(pos3 + 1, pos2).split(":");
                if (truePara.length != 2)
                    continue;

                sql.append(truePara[0]).append(" ").append(truePara[1]).append(PARA_SPLITTER);
            }
            else {
                String[] truePara = para.substring(pos1 + 1, pos2).split(":");
                if (truePara.length != 2)
                    continue;

                sql.append(truePara[0]).append(" ").append(truePara[1]).append(PARA_SPLITTER);
            }
        }

        if (sql.length() > 0) {
            sql.delete(sql.length() - 1, sql.length());
            sql.insert(0, "ORDER BY ");
        }

        return sql.toString();
    }

    /**
     * @param limit,  page limit
     * @param offset, data offset
     * @return
     */
    static String getLimitSql(int limit, int offset) {
        if (limit <= 0)
            limit = MAX_SIZE;

        if (offset < 0)
            offset = 0;

        StringBuilder sb = new StringBuilder();
        sb.append("LIMIT ").append(offset).append(", ").append(limit);

        return sb.toString();
    }

    /**
     * @param tablename, table name
     * @param paraStr,   [col:opt:org_id], ... ,[col:opt:org_id]
     *                   opt: like, =, >, <, >=, <=
     * @return
     */
    static String querySql(String tablename, String paraStr) {
        if (paraStr == null)
            return "";

        StringBuilder sql = new StringBuilder();

        List<String> paras = splitParas(paraStr);
        for (String para : paras) {
            if (para.trim().isEmpty())
                continue;

            int pos1 = para.indexOf('[');
            int pos2 = para.indexOf(']');

            if (pos1 < 0 || pos2 < 0 || pos1 > pos2)
                continue;

            int pos3 = para.indexOf('~');
            if (pos3 < 0) {
                String subParaStr = para.substring(pos1+1, pos2);
                String subSql = parseQueryPara(tablename, subParaStr);
                if (!subSql.isEmpty()) {
                    sql.append("(").append(subSql).append(")");
                    sql.append(AND);
                }
            }
            else if (pos3 > pos1+1) {
                String table = para.substring(pos1 + 1, pos3);

                String subParaStr = para.substring(pos3 + 1, pos2);
                String subSql = parseQueryPara(table, subParaStr);
                if (!subSql.isEmpty()) {
                    sql.append("(").append(subSql).append(")");
                    sql.append(AND);
                }
            }
        }

        if (sql.length() > 0) {
            int lastIndex = sql.lastIndexOf(AND);
            sql.delete(lastIndex, sql.length());
        }

        return sql.toString();
    }

    static String[] parseSingleQueryPara2array(String para) {
        String[] paras = new String[3];

        int pos = 0;
        for (int i=0; i<2; i++) {
            int tail = para.indexOf(':', pos);
            if (pos < 0)
                return null;

            paras[i] = para.substring(pos, tail);
            pos = tail+1;
        }

        if (pos < para.length())
            paras[2] = para.substring(pos);

        return paras;
    }

    static String parseQueryPara(String tableName, String paraStr) {
        String[] paras = paraStr.split("\\|");

        StringBuilder sql = new StringBuilder();

        for (int i=0; i<paras.length; i++) {
            String[] truePara = parseSingleQueryPara2array(paras[i]);
            if (truePara == null)
                continue;

            String col = truePara[0].trim();
            String opt = truePara[1].trim().toLowerCase();
            String value = truePara[2].trim();
            if (col.isEmpty() || value.isEmpty() || opt.isEmpty())
                continue;

            if (!StringUtils.isNullOrEmpty(tableName))
                sql.append(tableName).append("_");

            if (opt.equalsIgnoreCase(BIT_OPT.XOR)) {
                opt = "^" + opt.substring(BIT_OPT.XOR.length());
            }
            else if (opt.equalsIgnoreCase(BIT_OPT.OR)) {
                opt = "|" + opt.substring(BIT_OPT.OR.length());
            }
            else if (opt.equalsIgnoreCase(BIT_OPT.AND)) {
                opt = "&" + opt.substring(BIT_OPT.AND.length());
            }
            else if (opt.equalsIgnoreCase(LIKE_OPT.L_LIKE)) {
                opt = "like";
                value = "%" + value;
            }
            else if (opt.equalsIgnoreCase(LIKE_OPT.R_LIKE)) {
                opt = "like";
                value = value + "%";
            }
            else if (opt.equalsIgnoreCase(LIKE_OPT.B_LIKE)) {
                opt = "like";
                value = "%" + value + "%";
            }

            sql.append(col).append(" ").append(opt).append(" ").append("'").append(value).append("'").append(OR);
        }

        if (sql.length() > 0){
            int lastIndex = sql.lastIndexOf(OR);
            sql.delete(lastIndex, sql.length());
        }

        return sql.toString();
    }

    private static Pattern splitPattern = Pattern.compile("\\[(.*?)\\]");
    public static List<String> splitParas(String para) {
        Matcher matcher = splitPattern.matcher(para);

        List<String> paras = new ArrayList<>();

        while (matcher.find()) {
            log.trace(matcher.group());
            paras.add(matcher.group());
        }

        return paras;
    }
}

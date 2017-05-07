package com.jd.may.second;

import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * Created by wanghl on 17-5-7.
 */
public class ItemBuyDays {
    private static final Logger log = Logger.getLogger(ItemBuyDays.class);

    public static Map<String, Integer> itemBuyDays(String start, String end){
        Map<String, Integer> map = new HashMap<>();
        String sql = "select sku_id,group_concat(action_date) as date from user_action_1 where buy>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        log.info("sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            Set<String> dates = new HashSet<>();
            String skuId = String.valueOf(row.get("sku_id"));
            String buyDates = (String) row.get("date");
            String[] parts = buyDates.split(",");
            for(String part : parts){
                if(StringUtils.isNotBlank(part)){
                    dates.add(part);
                }
            }

            map.put(skuId, dates.size());
        }

        return map;
    }

    public static Map<String, Integer> itemIsBuy(String start, String end){
        Map<String, Integer> map = new HashMap<>();
        String sql = "select distinct(sku_id) from user_action_1 where buy>0 and action_date>='" + start + "' and action_date<='" + end + "'";
        log.info("sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String skuId = String.valueOf(row.get("sku_id"));
            map.put(skuId, 1);
        }

        return map;
    }

    public static Map<String, Integer> userIsBuy(String start, String end){
        Map<String, Integer> map = new HashMap<>();
        String sql = "select distinct(user_id) from user_action_1 where buy>0 and action_date>='" + start + "' and action_date<='" + end + "'";
        log.info("sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String skuId = String.valueOf(row.get("user_id"));
            map.put(skuId, 1);
        }

        return map;
    }
}


























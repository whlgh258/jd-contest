package com.jd.may.fifteen;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-5-7.
 */
public class ItemBuyDays {
    private static final Logger log = Logger.getLogger(ItemBuyDays.class);

    public static Map<String, Integer> itemBuyDays(String tablename){
        Map<String, Integer> map = new HashMap<>();
        String sql = "select sku_id,group_concat(action_date) as date from " + tablename + " where buy>0 group by sku_id";
        log.info("item buy days sql: " + sql);
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

    public static Map<String, Integer> itemIsBuy(String tablename){
        Map<String, Integer> map = new HashMap<>();
        String sql = "select distinct(sku_id) from " + tablename + " where buy>0";
        log.info("item is buy sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String skuId = String.valueOf(row.get("sku_id"));
            map.put(skuId, 1);
        }

        return map;
    }

    public static Map<String, Integer> userIsBuy(String tablename){
        Map<String, Integer> map = new HashMap<>();
        String sql = "select distinct(user_id) from " + tablename + " where buy>0";
        log.info("user is buy sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String skuId = String.valueOf(row.get("user_id"));
            map.put(skuId, 1);
        }

        return map;
    }
}


























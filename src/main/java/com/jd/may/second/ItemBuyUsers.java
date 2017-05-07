package com.jd.may.second;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-5-7.
 */
public class ItemBuyUsers {
    private static final Logger log = Logger.getLogger(ItemBuyUsers.class);

    public static Map<String, Double> itemUserCount(String sql, String key){
        Map<String, Double> map = new HashMap<>();
        log.info("sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String skuId = String.valueOf(row.get(key));
            double count = (double) row.get("count");

            map.put(skuId, count);
        }

        return map;
    }
}




























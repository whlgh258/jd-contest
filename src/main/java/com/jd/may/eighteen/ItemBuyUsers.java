package com.jd.may.eighteen;

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

    public static Map<String, Map<String, Double>> itemUserCount(String sql, String key, int actionPeriod, String mapKey){
        Map<String, Map<String, Double>> retMap = new HashMap<>();
        log.info("sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String id = String.valueOf(row.get(key));
            double count = (double) row.get("count");

            Map<String, Double> map = new HashMap<>();
            map.put(mapKey + actionPeriod, count);
            retMap.put(id, map);
        }

        return retMap;
    }
}




























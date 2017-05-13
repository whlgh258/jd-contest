package com.jd.may.second;

import multithreads.DBOperation;
import org.apache.hadoop.hdfs.web.resources.DoAsParam;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-5-6.
 */
public class Features {
    private static final Logger log = Logger.getLogger(Features.class);

    public static Map<String, Map<String, Double>> countFeature(String sql, int type, boolean isRatio){
        return work(sql, type, isRatio);
    }

    public static Map<String, Map<String, Double>> sumFeature(String sql, int type, boolean isRatio){
        return work(sql, type, isRatio);
    }

    public static Map<String, Map<String, Double>> avgFeature(String sql, int type, boolean isRatio){
        return work(sql, type, isRatio);
    }

    public static Map<String, Map<String, Double>> buyRatioFeature(String sql, int type, boolean isRatio){
        return work(sql, type, isRatio);
    }

    public static Map<String, Map<String,Map<String, Double>>> sumFeatureForCross(String sql){
        Map<String, Map<String, Map<String, Double>>> map = new HashMap<>();
        log.info("sum cross sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        for(Map<String, Object> row : result){
            String userId = String.valueOf(row.get("user_id"));
            String skuId = String.valueOf(row.get("sku_id"));

            double click = (double)row.get("click");
            double detail = (double)row.get("detail");
            double cart = (double)row.get("cart");
            double cartDelete = (double)row.get("cart_delete");
            double buy = (double)row.get("buy");
            double follow = (double)row.get("follow");

            Map<String, Double> userMap = new HashMap<>();
            userMap.put("click", click);
            userMap.put("detail", detail);
            userMap.put("cart", cart);
            userMap.put("cartDelete", cartDelete);
            userMap.put("buy", buy);
            userMap.put("follow", follow);

            Map<String, Double> itemMap = new HashMap<>();
            itemMap.put("click", click);
            itemMap.put("detail", detail);
            itemMap.put("cart", cart);
            itemMap.put("cartDelete", cartDelete);
            itemMap.put("buy", buy);
            itemMap.put("follow", follow);

            if(!map.containsKey(userId)){
                map.put(userId, new HashMap<>());
            }

            map.get(userId).put(userId + "_" + skuId, userMap);

            if(!map.containsKey(skuId)){
                map.put(skuId, new HashMap<>());
            }

            map.get(skuId).put(userId + "_" + skuId, itemMap);

        }

        return map;
    }

    private static Map<String, Map<String, Double>> work(String sql, int type, boolean isRatio){
        Map<String, Map<String, Double>> retmap = new HashMap<>();
        log.info("sql: " + sql);
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("size: " + result.size());
        String key = "";
        for(Map<String, Object> row : result){
            switch (type){
                case 0:
                    key = String.valueOf(row.get("user_id"));
                    break;
                case 1:
                    key = String.valueOf(row.get("sku_id"));
                    break;
                case 2:
                    String userId = String.valueOf(row.get("user_id"));
                    String skuId = String.valueOf(row.get("sku_id"));
                    key = userId + "_" + skuId;
            }

            double click = 0;
            double detail = 0;
            double cart = 0;
            double cartDelete = 0;
            double buy = 0;
            double follow = 0;

            if(isRatio){
                click = ((BigDecimal)row.get("click")).doubleValue();
                detail = ((BigDecimal)row.get("detail")).doubleValue();
                cart = ((BigDecimal)row.get("cart")).doubleValue();
                cartDelete = ((BigDecimal)row.get("cart_delete")).doubleValue();
                buy = ((BigDecimal)row.get("buy")).doubleValue();
                follow = ((BigDecimal)row.get("follow")).doubleValue();
            }
            else {
                click = (double)row.get("click");
                detail = (double)row.get("detail");
                cart = (double)row.get("cart");
                cartDelete = (double)row.get("cart_delete");
                buy = (double)row.get("buy");
                follow = (double)row.get("follow");
            }

            Map<String, Double> map = new HashMap<>();
            map.put("click", click);
            map.put("detail", detail);
            map.put("cart", cart);
            map.put("cartDelete", cartDelete);
            map.put("buy", buy);
            map.put("follow", follow);

            retmap.put(key, map);
        }

        return retmap;
    }
}

































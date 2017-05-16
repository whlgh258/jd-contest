package com.jd.may.fifteen;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-5-7.
 */
public class Popular {
    private static final Logger log = Logger.getLogger(Popular.class);

    public static Map<String, Double> userPopular(String tablename, int labelPeriod, int actionPeriod){
        String sql = "select user_id,round(log(sum(click)*0.01+sum(buy)*5+sum(detail)*0.1+sum(cart)*2+sum(cart_delete)+sum(follow)*3 + 1),3) as popular from " + tablename + " where action_period=" + actionPeriod + " group by user_id";
        log.info("user popular sql: " + sql);

        return popular(sql, "user_id", labelPeriod, actionPeriod);
    }

    public static Map<String, Double> itemPopular(String tablename, int labelPeriod, int actionPeriod){
        String sql = "select sku_id,round(log(sum(click)*0.01+sum(buy)*5+sum(detail)*0.1+sum(cart)*2+sum(cart_delete)+sum(follow)*3 + 1),3) as popular from " + tablename + " where action_period=" + actionPeriod + " group by sku_id";

        log.info("item popular sql: " + sql);

        return popular(sql, "sku_id", labelPeriod, actionPeriod);
    }

    public static Map<String, Double> itemActionUserCount(String tablename, int labelPeriod, int actionPeriod){
        String sql = "select sku_id,round(log(count(distinct user_id) + 1),3) as popular from " + tablename + " where action_period=" + actionPeriod + " group by sku_id";
        log.info("item action user count sql: " + sql);
        return popular(sql, "sku_id", labelPeriod, actionPeriod);
    }

    private static Map<String, Double> popular(String sql, String key, int labelPeriod, int actionPeriod){
        Map<String, Double> map = new HashMap<>();
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        for(Map<String, Object> row : result){
            String id = String.valueOf(row.get(key));
            double popular = (Double) row.get("popular");
            map.put(id + "_" + labelPeriod + "_" + actionPeriod, popular);
        }

        return map;
    }
}

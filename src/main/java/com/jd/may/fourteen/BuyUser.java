package com.jd.may.fourteen;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-5-14.
 */
public class BuyUser {
    private static final Logger log = Logger.getLogger(BuyUser.class);

    public static void main(String[] args) {
        String buySql = "select distinct user_id from user_action_1 where buy>0";
        log.info("buy sql: " + buySql);

        List<Map<String, Object>> buyResult = DBOperation.queryBySql(buySql);
        log.info("buy size: " + buyResult.size());

        int all = 0;
        for(Map<String, Object> row : buyResult){
            String userId = String.valueOf(row.get("user_id"));

            String allSql = "select * from user_action_1 where user_id=" + userId;
            log.info("user sql: " + allSql);
            List<Map<String, Object>> result = DBOperation.queryBySql(allSql);
            log.info("user " + userId + " size: " + result.size());
            for(Map<String, Object> allRow : result){
                ++all;
                allRow.remove("id");
                allRow.remove("last_comment_date");
                allRow.remove("reg_date");
                DBOperation.insert("user_action_2", allRow);
            }
        }

        log.info("all size: " + all);
    }
}
























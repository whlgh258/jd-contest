package com.jd.may.fourteen;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-5-14.
 */
public class NotBuyUser {
    private static final Logger log = Logger.getLogger(NotBuyUser.class);

    public static void main(String[] args) {
        Map<String, String> userMap = new HashMap<>();
        String userSql = "select distinct user_id from user_action_2";
        List<Map<String, Object>> userResult = DBOperation.queryBySql(userSql);
        log.info("user size: " + userResult.size());
        for(Map<String, Object> row : userResult){
            String userId = String.valueOf(row.get("user_id"));
            userMap.put(userId, "1");
        }

        String buySql = "select distinct user_id from user_action_1 where buy=0";
        log.info("buy sql: " + buySql);

        List<Map<String, Object>> buyResult = DBOperation.queryBySql(buySql);
        log.info("buy size: " + buyResult.size());

        int all = 0;
        for(Map<String, Object> row : buyResult){
            String userId = String.valueOf(row.get("user_id"));
            if(userMap.containsKey(userId)){
                continue;
            }

            String allSql = "select * from user_action_1 where user_id=" + userId;
            log.info("user sql: " + allSql);
            List<Map<String, Object>> result = DBOperation.queryBySql(allSql);
            log.info("user " + userId + " size: " + result.size());
            for(Map<String, Object> allRow : result){
                ++all;
                allRow.remove("id");
                allRow.remove("last_comment_date");
                allRow.remove("reg_date");
                DBOperation.insert("user_action_3", allRow);
            }
        }

        log.info("all size: " + all);
    }
}

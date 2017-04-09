package com.jd.contest;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-9.
 */
public class TrainData {
    private static final Logger log = Logger.getLogger(TrainData.class);

    public static void main(String[] args) {
        String positiveSql = "select user_id,sku_id from user_action where buy>0 and last_buy_date>='2016-04-11'";
        List<Map<String, Object>> positiveResult = DBOperation.queryBySql(positiveSql);
        log.info("positive instances: " + positiveResult.size());
        int i = 0;
        for(Map<String, Object> positiveRow : positiveResult){
            int userId = (int) positiveRow.get("user_id");
            int productId = (int) positiveRow.get("sku_id");

            String querySql = "select click,detail,cart,cart_delete,buy,follow,age,user_level,attr1,attr2,attr3,cate,brand,has_bad_comment,has_buy from " +
                    "user_action where last_action_date<='2016-04-10' and user_id=" + userId + " and sku_id=" + productId;
//            log.info(querySql);
            List<Map<String, Object>> queryResult = DBOperation.queryBySql(querySql);
            if(queryResult.size() > 0){
                Map<String, Object> queryRow = queryResult.get(0);
                if(queryRow.size() > 0){
                    ++i;
                }
            }
        }

        log.info("i = " + i);

        int j = 0;
        String falseSql = "select user_id,sku_id from user_action where buy=0 and last_buy_date>='2016-04-11'";
        List<Map<String, Object>> falseResult = DBOperation.queryBySql(falseSql);
        log.info("false instances: " + falseResult.size());
        for(Map<String, Object> falseRow : falseResult){
            int userId = (int) falseRow.get("user_id");
            int productId = (int) falseRow.get("sku_id");

            String querySql = "select click,detail,cart,cart_delete,buy,follow,age,user_level,attr1,attr2,attr3,cate,brand,has_bad_comment,has_buy from " +
                    "user_action where last_action_date<='2016-04-10' and user_id=" + userId + " and sku_id=" + productId;
            List<Map<String, Object>> queryResult = DBOperation.queryBySql(querySql);
            if(queryResult.size() > 0){
                Map<String, Object> queryRow = queryResult.get(0);
                if(queryRow.size() > 0){
                    ++j;
                }
            }
        }

        log.info("j = " + j);
    }
}

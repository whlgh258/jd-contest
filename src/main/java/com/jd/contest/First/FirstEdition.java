package com.jd.contest.First;

import au.com.bytecode.opencsv.CSVWriter;
import jdk.nashorn.internal.runtime.ECMAException;
import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-9.
 */
public class FirstEdition {
    private static final Logger log = Logger.getLogger(FirstEdition.class);

    public static void main(String[] args) throws Exception{
        Connection conn = DBConnection.getConnection();

        List<Integer> userIds = new ArrayList<>();
        String userInfoSql = "select distinct user_id from jd_contest.user order by user_id";
        List<Map<String, Object>> userInfoResult = DBOperation.queryBySql(conn, userInfoSql);
        for (Map<String, Object> userInfoRow : userInfoResult) {
            int userId = (int) userInfoRow.get("user_id");
            userIds.add(userId);
        }

        Map<Integer, Integer> productIds = new HashMap<>();
        String productSql = "select distinct sku_id from jd_contest.product order by sku_id";
        List<Map<String, Object>> productResult = DBOperation.queryBySql(conn, productSql);
        for (Map<String, Object> productRow : productResult) {
            productIds.put((int) productRow.get("sku_id"), 1);
        }

        Map<Integer, Integer> buyMap = new HashMap<>();
        for(int userId : userIds){
            log.info("user id: " + userId);
            String buySql = "select sku_id from jd_contest.user_action where buy>0 and user_id=" + userId + " and sku_id in(select sku_id from jd_contest.product) order by buy desc limit 1";
            log.info(buySql);
            List<Map<String, Object>> buyResult = DBOperation.queryBySql(conn, buySql);
            if(buyResult.size() > 0){
                Map<String, Object> buyRow = buyResult.get(0);
                if(buyRow.size() > 0){
                    int productId = (int) buyRow.get("sku_id");
                    if(productIds.containsKey(productId)){
                        buyMap.put(userId, productId);
                    }
                }
            }
        }

        log.info("user ids: " + userIds.size());
        log.info("product ids: " + productIds.size());
        log.info("has buy user and product: " + buyMap.size());

        CSVWriter writer = new CSVWriter( new FileWriter("/home/wanghl/jd_contest/result.csv"), ',', '\0');
        List<String> columns = new ArrayList<String>();
        columns.add("user_id");
        columns.add("sku_id");

        writer.writeNext( columns.toArray( new String[0] ) );

        for(Map.Entry<Integer, Integer> entry : buyMap.entrySet()){
            String[] array = new String[2];
            array[0] = entry.getKey() + "";
            array[1] = entry.getValue() + "";

            writer.writeNext(array);
        }

        writer.close();
        DBConnection.close(conn);
    }
}

package com.jd.contest;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.io.FileWriter;
import java.sql.Connection;
import java.util.*;

/**
 * Created by wanghl on 17-4-8.
 */
public class ProductBuyedSimilarityFile {
    private static final Logger log = Logger.getLogger(ProductBuyedSimilarityFile.class);

    public static void main(String[] args) throws Exception {
        Connection conn = DBConnection.getConnection();

        List<Integer> userIds = new ArrayList<>();
        String userInfoSql = "select distinct user_id from jd_contest.user_action where buy>0 order by user_id";
        List<Map<String, Object>> userInfoResult = DBOperation.queryBySql(conn, userInfoSql);
        for (Map<String, Object> userInfoRow : userInfoResult) {
            int userId = (int) userInfoRow.get("user_id");
            userIds.add(userId);
        }

        List<Integer> productIds = new ArrayList<>();
        String productSql = "select distinct sku_id from jd_contest.user_action where buy>0 order by sku_id";
        List<Map<String, Object>> productResult = DBOperation.queryBySql(productSql);
        for (Map<String, Object> productRow : productResult) {
            productIds.add((int) productRow.get("sku_id"));
        }

        Map<String, Integer> buyMap = new HashMap<>();
        String buySql = "select user_id,sku_id,buy from jd_contest.user_action where buy>0";
        List<Map<String, Object>> buyResult = DBOperation.queryBySql(conn, buySql);
        for(Map<String, Object> buyRow : buyResult){
            int userId = (int) buyRow.get("user_id");
            int productId = (int) buyRow.get("sku_id");
            int buy = (int) buyRow.get("buy");
            String key = userId + "_" + productId;
            buyMap.put(key, buy);
        }

        log.info("user ids: " + userIds.size());
        log.info("product ids: " + productIds.size());
        log.info("has buy user and product: " + buyMap.size());

        CSVWriter writer = new CSVWriter( new FileWriter("/home/wanghl/jd_contest/product_buyed_user.csv"), ',', ' ');
        List<String> columns = new ArrayList<String>();
        columns.add("sku_id");
        for(int userId : userIds){
            columns.add(userId + "");
        }

        writer.writeNext( columns.toArray( new String[0] ) );

        for(int productId : productIds){
            String[] array = new String[userIds.size() + 1];
            array[0] = productId + "";
            for(int i = 0; i < userIds.size(); i++){
                int userId = userIds.get(i);
                String key = userId + "_" + productId;
                Integer buy = buyMap.get(key);
                if(null != buy){
                    array[i + 1] = buy + "";
                }
            }

            writer.writeNext(array);
        }

        writer.close();
        DBConnection.close(conn);
    }
}






















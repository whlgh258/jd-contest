package com.jd.may.second;

import java.io.FileWriter;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBOperation;
import org.apache.log4j.Logger;
import org.apache.spark.mllib.tree.impurity.Entropy;

/**
 * Created by wanghl on 17-5-9.
 */
public class CartNotBuy {
    private static final Logger log = Logger.getLogger(CartNotBuy.class);

    public static void main(String[] args) throws Exception {
        String sql = "select user_id,sku_id,sum(cart) as cart,sum(buy) as buy from user_action_1 where action_date>='2016-04-06' and action_date<='2016-04-15' and cart>0 group by user_id,sku_id";
        Map<String, List<String>> map = new HashMap<>();
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        log.info("result size: " + result.size());
        for(Map<String, Object> row : result){
            String userId = String.valueOf(row.get("user_id"));
            String skuId = String.valueOf(row.get("sku_id"));
            int cart = ((BigDecimal) row.get("cart")).intValue();
            int buy = ((BigDecimal) row.get("buy")).intValue();
            if(0 == buy || (0 != buy && buy < cart)){
                if(!map.containsKey(userId)){
                    map.put(userId, new ArrayList<String>());
                }

                map.get(userId).add(skuId);
            }
        }

        log.info("map size: " + map.size());
        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/result.csv"), ',' , '\0');
        writer.writeNext("user_id", "sku_id");
        for(Entry<String, List<String>> entry : map.entrySet()){
            /*System.out.print(entry.getKey() + ": ");h2o
            for(String skuId : entry.getValue()){
                System.out.print(skuId + ", ");
            }
            System.out.println();*/
            String userId = entry.getKey();
            List<String> value = entry.getValue();
            writer.writeNext(userId, value.get(0));
        }

        writer.close();
    }
}




























package com.jd.may.eighteen;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by wanghl on 17-5-21.
 */
public class Result {
    private static Logger log = Logger.getLogger(Result.class);

    public static void main(String[] args) throws Exception {
        String path = "/home/wanghl/jd_contest/0519/result";
        File dir = new File(path);

        Map<String, Double> map = new HashMap<>();
        Map<String, Double> result = new HashMap<>();
        for(String filename : dir.list()){
            CSVReader reader = new CSVReader(new FileReader(path + "/" + filename));
            List<String[]> lines = reader.readAll();
            for(String[] line : lines){
                if(line[0].contains("id")){
                    continue;
                }

                String userId = line[0];
                String skuId = line[1];
                double prob = Double.parseDouble(line[3]);

                if(map.containsKey(userId)){
                    if(prob > map.get(userId)){
                        map.put(userId, prob);
                        result.put(userId + "_" + skuId, prob);
                    }
                }
                else {
                    map.put(userId, prob);
                    result.put(userId + "_" + skuId, prob);
                }
            }

            reader.close();
        }

        List<Entry<String, Double>> list = new ArrayList<>();
        list.addAll(result.entrySet());
        Collections.sort( list, new Comparator<Entry<String, Double>>()
        {
            public int compare( Entry<String, Double> o1, Entry<String, Double> o2 )
            {
                return (o2.getValue() - o1.getValue() > 0 ? 1 : -1); // 降序
            }
        } );

        Set<String> users = new HashSet<>();
        List<String[]> allLines = new ArrayList<>();
        for(Entry<String, Double> entry : list){
            String userId = entry.getKey().split("_")[0];
            String skuId = entry.getKey().split("_")[1];
            if(!users.contains(userId)){
                users.add(userId);
                String[] line = new String[2];
                line[0] = userId;
                line[1] = skuId;
                allLines.add(line);
            }
        }

        System.out.println(users.size());
//        allLines = allLines.subList(0, 1500);

        List<String[]> lines = new ArrayList<>();
        for(String[] line : allLines){
            String userId = line[0];
            String skuId = line[1];
            String sql = "select count(1) as count from (select sum(cart) as sum_cart,sum(buy) as sum_buy from user_action_1 where user_id=" + userId + " and sku_id=" + skuId + ") a where sum_cart>0 and sum_buy=0";
            log.info(sql);
            List<Map<String, Object>> rs = DBOperation.queryBySql(sql);
            long count = (long) rs.get(0).get("count");
            if(count > 0){
                lines.add(line);
            }
        }

        System.out.println(lines.size());
        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/0519/result.csv"), ',', '\0');
        writer.writeNext(new String[]{"user_id", "sku_id"});
        writer.writeAll(lines);

        writer.close();
    }
}





























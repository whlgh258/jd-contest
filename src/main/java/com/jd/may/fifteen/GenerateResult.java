package com.jd.may.fifteen;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-5-15.
 */
public class GenerateResult {
    private static Logger log = Logger.getLogger(GenerateResult.class);

    public static void main(String[] args) throws Exception {
        CSVReader reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/0513/result.csv"));
        List<String[]> lines = reader.readAll();
        Map<String, Double> map = new HashMap<>();
        Map<String, String> result = new HashMap<>();
        for(String[] line : lines){
            if(line[0].contains("id")){
                continue;
            }

            String userId = line[0];
            String skuId = line[1];

            String sql = "select * from user_action_3 where action_date>='2016-04-15' and user_id=" + userId + " and sku_id=" + skuId;
            List<Map<String, Object>> queryResult = DBOperation.queryBySql(sql);
            if(0 == queryResult.size()){
                continue;
            }


            double prob = Double.parseDouble(line[3]);

            if(map.containsKey(userId)){
                if(prob > map.get(userId)){
                    map.put(userId, prob);
                    result.put(userId, skuId);
                }
            }
            else {
                map.put(userId, prob);
                result.put(userId, skuId);
            }
        }

        List<String[]> allLines = new ArrayList<>();
        for(Map.Entry<String, String> entry : result.entrySet()){
            String[] line = new String[2];
            line[0] = entry.getKey();
            line[1] = entry.getValue();
            allLines.add(line);
        }

        log.info("result size: " + allLines.size());
        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/0513/final_result.csv"), ',', '\0');
        writer.writeNext(new String[]{"user_id", "sku_id"});
        writer.writeAll(allLines);

        writer.close();
        reader.close();
    }
}

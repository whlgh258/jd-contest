package com.jd.april.thirty;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-30.
 */
public class BuyGenerator {
    private static final Logger log = Logger.getLogger(BuyGenerator.class);

    public static void main(String[] args) throws Exception {
        Map<String, String> train = new HashMap<>();
        Map<String, String> predict = new HashMap<>();
        Map<String, String> userMap = new HashMap<>();

        CSVReader reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/0430/final.csv"));
        List<String[]> lines = reader.readAll();
        for(String[] line : lines) {
            if (line[0].contains("id")) {
                continue;
            }

            String userId = line[0];
            String skuId = line[1];
            train.put(userId + "_" + skuId, skuId);
        }

        reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/0430/notBuyResult.csv"));
        lines = reader.readAll();
        for(String[] line : lines) {
            if (line[0].contains("id")) {
                continue;
            }

            String userId = line[0];
            String skuId = line[1];
            predict.put(userId + "_" + skuId, skuId);
        }

        List<String[]> result = new ArrayList<>();
        for(Map.Entry<String, String> entry : train.entrySet()){
            String key = entry.getKey();
            if(!predict.containsKey(key)){
                String[] line = new String[2];
                String[] parts = key.split("_");
                String userId = parts[0];
                String skuId = parts[1];
                if(!userMap.containsKey(userId)){
                    userMap.put(userId, "1");

                    line[0] = userId;
                    line[1] = skuId;

                    result.add(line);
                }
            }
        }

        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/0430/buy_result.csv"), ',', '\0');
        writer.writeNext(new String[]{"user_id", "sku_id"});
        writer.writeAll(result);

        writer.close();
        reader.close();
    }
}

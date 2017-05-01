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
public class ResultGenerator {
    private static final Logger log = Logger.getLogger(ResultGenerator.class);

    public static void main(String[] args) throws Exception {
        CSVReader reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/0430/finalResult.csv"));
        List<String[]> lines = reader.readAll();
        Map<String, Double> map = new HashMap<>();
        Map<String, String> result = new HashMap<>();
        for(String[] line : lines){
            if(line[0].contains("id")){
                continue;
            }

            String userId = line[0];
            String skuId = line[1];
            double prob = Double.parseDouble(line[2]);

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

        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/0430/res.csv"), ',', '\0');
        writer.writeNext(new String[]{"user_id", "sku_id"});
        writer.writeAll(allLines);

        writer.close();
        reader.close();
    }
}

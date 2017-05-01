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
public class NotBuyGenerator {
    private static final Logger log = Logger.getLogger(NotBuyGenerator.class);

    public static void main(String[] args) throws Exception {
        CSVReader reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/0430/finalResult.csv"));
        List<String[]> lines = reader.readAll();
        log.info("size: " + lines.size());
        Map<String, Double> map = new HashMap<>();
        Map<String, Double> result = new HashMap<>();
        for(String[] line : lines){
            if(line[0].contains("id")){
                continue;
            }

            String userId = line[0];
            String skuId = line[1];
            double prob = Double.parseDouble(line[2]);
            String key = userId + "_" + skuId;

            if(map.containsKey(key)){
                if(prob > map.get(key)){
                    map.put(key, prob);
                    result.put(key, prob);
                }
            }
            else {
                map.put(key, prob);
                result.put(key, prob);
            }
        }

        List<String[]> allLines = new ArrayList<>();
        for(Map.Entry<String, Double> entry : result.entrySet()){
            String[] line = new String[3];
            String[] parts = entry.getKey().split("_");
            line[0] = parts[0];
            line[1] = parts[1];
            line[2] = String.valueOf(entry.getValue());
            allLines.add(line);
        }

        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/0430/notBuyResult.csv"), ',', '\0');
        writer.writeNext(new String[]{"user_id", "sku_id"});
        writer.writeAll(allLines);

        writer.close();
        reader.close();

    }
}

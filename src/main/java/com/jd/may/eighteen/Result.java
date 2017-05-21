package com.jd.may.eighteen;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by wanghl on 17-5-21.
 */
public class Result {
    private static Logger log = Logger.getLogger(Result.class);

    public static void main(String[] args) throws Exception {
        String path = "/home/wanghl/jd_contest/0519/result";
        File dir = new File(path);

        Set<String[]> allLines = new HashSet<>();
        Map<String, Double> map = new HashMap<>();
        Map<String, String> result = new HashMap<>();
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
                        result.put(userId, skuId);
                    }
                }
                else {
                    map.put(userId, prob);
                    result.put(userId, skuId);
                }
            }

            reader.close();
        }

        for(Map.Entry<String, String> entry : result.entrySet()){
            String[] line = new String[2];
            line[0] = entry.getKey();
            line[1] = entry.getValue();
            allLines.add(line);
        }


        List<String[]> all = new ArrayList<>(allLines);
        CSVWriter writer = new CSVWriter(new FileWriter("/home/wanghl/jd_contest/0519/result.csv"), ',', '\0');
        writer.writeNext(new String[]{"user_id", "sku_id"});
        writer.writeAll(all);

        writer.close();
    }
}





























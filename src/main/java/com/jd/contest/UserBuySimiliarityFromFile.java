package com.jd.contest;

import au.com.bytecode.opencsv.CSVReader;
import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import java.io.FileReader;
import java.sql.Connection;
import java.util.*;

/**
 * Created by wanghl on 17-4-8.
 */
public class UserBuySimiliarityFromFile {
    private static final Logger log = Logger.getLogger(UserBuySimiliarityFromFile.class);

    public static void main(String[] args) throws Exception {
        Connection conn = DBConnection.getConnection();
        CSVReader reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/user_buy_product.csv"));
        List<String[]> lines = reader.readAll();
        log.info("total lines: " + lines.size());
        List<double[]> rows = new ArrayList<>(lines.size());
        for(int i = 1; i < lines.size(); i++){
            String[] line = lines.get(i);
            double[] array = new double[line.length];
            for(int j = 0; j < line.length; j++){
                String part = line[j];
                if(StringUtils.isNotBlank(part)){
                    array[j] = Integer.parseInt(part.trim());
                }
            }

            rows.add(array);
        }

        for(int i = 0; i < rows.size() - 1; i++){
            List<Map<String, Object>> insertList = new ArrayList<>();
            double[] rowA = rows.get(i);
            int userIdA = (int) rowA[0];
            log.info(i + ", " + userIdA);
            double[] arrayA = Arrays.copyOfRange(rowA, 1, rowA.length);
            RealVector vectorA = new ArrayRealVector(arrayA);
            for(int j = i + 1; j < rows.size(); j++){
                double[] rowB = rows.get(j);
                int userIdB = (int) rowB[0];
                double[] arrayB = Arrays.copyOfRange(rowB, 1, rowB.length);
                RealVector vectorB = new ArrayRealVector(arrayB);
                double similarity = vectorA.cosine(vectorB);
                if(similarity > 0){
                    Map<String, Object> insertMap = new HashMap<>();
                    insertMap.put("user_idA", userIdA);
                    insertMap.put("user_idB", userIdB);
                    insertMap.put("similarity", similarity);

                    insertList.add(insertMap);
                }
            }

            DBOperation.insert(conn, "jd_contest.user_buy_similarity", insertList);
        }

        DBConnection.close(conn);
        reader.close();
    }
}

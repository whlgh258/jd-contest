package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;
import scala.Int;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-4.
 */
public class ProductImporter {
    private static final Logger log = Logger.getLogger(ProductImporter.class);

    public static void main(String[] args) throws Exception {
        String filename = "/home/wanghl/jd_contest/product.csv";

        BufferedReader br = null;
        Connection conn = DBConnection.getConnection();
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader(filename));
            String line = null;
            while(null != (line = br.readLine())){
                if(line.contains("id")){
                    continue;
                }
                String[] tokens = line.split(",");
                int productId = Integer.parseInt(tokens[0].trim());
                int attr1 = 0;
                String attr1Str = tokens[1].trim();
                try{
                    attr1 = Integer.parseInt(attr1Str);
                }
                catch (Exception e){
                    attr1 = 0;
                }
                if(attr1 < 0){
                    attr1 = 0;
                }

                int attr2 = 0;
                String attr2Str = tokens[2].trim();
                try{
                    attr2 = Integer.parseInt(attr2Str);
                }
                catch (Exception e){
                    attr2 = 0;
                }
                if(attr2 < 0){
                    attr2 = 0;
                }

                int attr3 = 0;
                String attr3Str = tokens[3].trim();
                try{
                    attr3 = Integer.parseInt(attr3Str);
                }
                catch (Exception e){
                    attr3 = 0;
                }
                if(attr3 < 0){
                    attr3 = 0;
                }

                int cate = Integer.parseInt(tokens[4].trim());
                int brand = Integer.parseInt(tokens[5].trim());

                Map<String, Object> row = new HashMap<>();
                row.put("sku_id", productId);
                row.put("attr1", attr1);
                row.put("attr2", attr2);
                row.put("attr3", attr3);
                row.put("cate", cate);
                row.put("brand", brand);

                rows.add(row);
            }

            DBOperation.insert(conn, "jd_contest.product", rows);
        }
        catch (Exception e){
            log.error(e.getMessage(), e);
        }
        finally {
            br.close();
            DBConnection.close(conn);
        }
    }
}

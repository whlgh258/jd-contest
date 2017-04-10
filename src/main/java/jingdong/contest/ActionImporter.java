package jingdong.contest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

/**
 * Created by wanghl on 17-4-2.
 */
public class ActionImporter {
    private static final Logger log = Logger.getLogger(ActionImporter.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String path = "/home/wanghl/jd_contest/";
        String[] filenames = {"action_02.csv", "action_03.csv", "action_04.csv"};
        List<Map<String, Object>> rows = new ArrayList<>();
        BufferedReader br = null;
        int i = 0;
        Connection conn = DBConnection.getConnection();

        Map<Integer, Integer> productIds = new HashMap<>();
        String sql = "select distinct sku_id from product";
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        for(Map<String, Object> row : result){
            int productId = (int) row.get("sku_id");
            productIds.put(productId, 1);
        }

        log.info("product size: " + productIds.size());

        Set<Integer> set = new HashSet<>();

        int j = 0;
        DateFormat format = DateFormat.getDateInstance();
        for(String filename : filenames){
            try {
                br = new BufferedReader(new FileReader(path + filename));
                String line = null;
                while(null != (line = br.readLine())){
                    if(line.contains("id")){
                        continue;
                    }
                    ++i;
                    String[] tokens = line.split(",");
                    String userStr = tokens[0].trim();
                    String part = "0";
                    if(userStr.contains(".")){
                        String[] parts = userStr.trim().split("\\.");
                        part = parts[0];
                    }
                    else {
                        part = userStr;
                    }
                    int userId = Integer.parseInt(part.trim());
                    int skuId = Integer.parseInt(tokens[1].trim());
                    if(productIds.containsKey(skuId)){
                        ++j;
                        set.add(skuId);

                        LocalDateTime time = LocalDateTime.parse(tokens[2].trim(), formatter);
                        Timestamp timestamp = Timestamp.valueOf(time);

                        Date date = new Date(timestamp.getTime());
                        String dateStr = format.format(date);

                        int modelId = 0;
                        if(StringUtils.isNotBlank(tokens[3])){
                            modelId = Integer.parseInt(tokens[3].trim());
                        }
                        int type = Integer.parseInt(tokens[4].trim());
                        int cate = Integer.parseInt(tokens[5].trim());
                        int brand = Integer.parseInt(tokens[6].trim());

                        Map<String, Object> row = new HashMap<>();
                        row.put("user_id", userId);
                        row.put("sku_id", skuId);
                        row.put("model_id", modelId);
                        row.put("type", type);
                        row.put("cate", cate);
                        row.put("brand", brand);
                        row.put("time", timestamp);
                        row.put("date", dateStr);
                        rows.add(row);

                        if(i % 1000000 == 0){
                            log.info("i: " + i);
                            DBOperation.insert(conn, "jd_contest.action_1", rows);
                            rows.clear();
                        }
                    }
                }
            }
            catch (Exception e){
                log.error(e.getMessage(), e);
            }
            finally {
                br.close();
            }
        }

        DBOperation.insert(conn, "jd_contest.action_1", rows);
        System.out.println("i = " + i);
        System.out.println("j = " + j);
        System.out.println("set size: " + set.size());
        DBConnection.close(conn);
    }
}

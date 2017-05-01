package jingdong.contest.tain;

import au.com.bytecode.opencsv.CSVWriter;
import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.util.*;

/**
 * Created by wanghl on 17-4-15.
 */
public class TrainInstance {
    private static final Logger log = Logger.getLogger(TrainInstance.class);
    private static final Random random = new Random();

    public static void main(String[] args) {
        Connection conn = DBConnection.getConnection();
        String countSql = "select count(1) as count from user_action_horizon";
        List<Map<String, Object>> countResult = DBOperation.queryBySql(conn, countSql);
        long count = (long) countResult.get(0).get("count");
        log.info("count: " + count);
        List<Map<String, Object>> trainPositiveInstances = new ArrayList<>();
        List<Map<String, Object>> trainFalseInstances = new ArrayList<>();
        Set<Map<String, Object>> set = new HashSet<>();
        int sum = 0;
        for (int i = 0; i < count; i += 10000) {
            String sql = "select * from user_action_horizon limit " + i + ",10000";
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            sum += result.size();
            log.info("size: " + sum);
            for (Map<String, Object> row : result) {
                int buy1 = (int) row.get("buy_1");
                int buy2 = (int) row.get("buy_2");
                int buy3 = (int) row.get("buy_3");
                int buy4 = (int) row.get("buy_4");
                int buy5 = (int) row.get("buy_5");

                int userId = (int) row.get("user_id");
                int skuId = (int) row.get("sku_id");

                /**
                 * 正例
                 * 4.11-4.15号购买过的且2.1-4.10有过行为的作为正例，2.1-4.10的行为作为feature
                 */
                if (buy1 > 0 || buy2 > 0 || buy3 > 0 || buy4 > 0 || buy5 > 0) {
                    boolean hasAction = false;
                    for(int j = 6; j <= 75; j++){
                        if((int) row.get("click_" + j) > 0 ||
                           (int) row.get("detail_" + j) > 0 ||
                           (int) row.get("cart_" + j) > 0 ||
                           (int) row.get("cart_delete_" + j) > 0 ||
                           (int) row.get("follow_" + j) > 0){

                            hasAction = true;
                            if(hasAction){
                                break;
                            }
                        }
                    }

                    if(true){
                        removeKey(row, 5);
                        row.put("buy", 1);
                        trainPositiveInstances.add(row);
                    }
                }
                /**
                 * 负例
                 * 4.11-4.15号未购买过，但2.1-4.10号有过动作的作为负例
                 * 正例1386,负例8倍 -> 11088
                 * 是找已购买过的用户还是所有用户作为负例？
                 */
                else if (0 == buy1 && 0 == buy2 && 0 == buy3 && 0 == buy4 && 0 == buy5) {
                    boolean hasAction = false;
                    for(int j = 6; j <= 75; j++){
                        if((int) row.get("click_" + j) > 0 ||
                           (int) row.get("detail_" + j) > 0 ||
                           (int) row.get("cart_" + j) > 0 ||
                           (int) row.get("cart_delete_" + j) > 0 ||
                           (int) row.get("follow_" + j) > 0){

                            hasAction = true;
                            if(hasAction){
                                break;
                            }
                        }
                    }

                    if(hasAction){
                        double rand = random.nextDouble();
                        if(rand > 0.5 && trainFalseInstances.size() < 11088){
                            removeKey(row, 5);
                            row.put("buy", 0);
                            trainFalseInstances.add(row);
                        }
                    }
                }
            }
        }

        log.info("positive: " + trainPositiveInstances.size());
        log.info("false: " + trainFalseInstances.size());
        set.addAll(trainPositiveInstances);
        set.addAll(trainFalseInstances);

        writeToFile(set);
        DBConnection.close(conn);
    }

    private static void removeKey(Map<String, Object> row, int k){
        for (int j = 1; j <= k; j++) {
            row.remove("click_" + j);
            row.remove("detail_" + j);
            row.remove("cart_" + j);
            row.remove("cart_delete_" + j);
            row.remove("buy_" + j);
            row.remove("follow_" + j);
        }
    }

    private static void writeToFile(Set<Map<String, Object>> set){
        Map<String, Object>[] array = set.toArray(new Map[0]);
        Map<String, Object> row = array[0];
        Set<String> keys = row.keySet();
        List<String> columns = new ArrayList<>();
        columns.addAll(keys);

        try {
            CSVWriter writer = new CSVWriter( new FileWriter("/home/wanghl/jd_contest/0416/train.csv"), ',', '\0');
            writer.writeNext(columns.toArray(new String[0]));

            for(Map<String, Object> line : set){
                List<String> elements = new ArrayList<>();
                for(String column : columns){
                    elements.add(String.valueOf(line.get(column)));
                }

                writer.writeNext(elements.toArray(new String[0]));
            }

            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

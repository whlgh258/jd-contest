import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-2.
 */
public class ActionImporter {
    private static final Logger log = Logger.getLogger(ActionImporter.class);
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {
        String path = "/home/wanghl/jd/";
        String[] filenames = {"action_02.csv", "action_03.csv", "action_03_extra.csv", "action_04.csv"};
        List<Map<String, Object>> rows = new ArrayList<>();
        BufferedReader br = null;
        int i = 0;
        Connection conn = DBConnection.getConnection();
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
                    int userId = Integer.parseInt(tokens[0].trim());
                    int skuId = Integer.parseInt(tokens[1].trim());
                    LocalDateTime time = LocalDateTime.parse(tokens[2].trim(), formatter);
                    Timestamp timestamp = Timestamp.valueOf(time);
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
                    rows.add(row);

                    if(i % 1000000 == 0){
                        log.info("i: " + i);
                        DBOperation.insert(conn, "jd.action", rows);
                        rows.clear();
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

        DBOperation.insert(conn, "jd.action", rows);
        System.out.println("i = " + i);
        DBConnection.close(conn);
    }
}

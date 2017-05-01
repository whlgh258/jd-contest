package jd.contest;

import com.alibaba.fastjson.JSONObject;
import multithreads.DBOperation;
import org.apache.log4j.Logger;
import scala.Int;

import java.sql.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-30.
 */
public class UpdateActionId {
    private static Logger log = Logger.getLogger(UpdateActionId.class);

    public static void main(String[] args) {
        String sql = "select user_id,sku_id,action_date from user_action_1 where click>0";
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        for(Map<String, Object> row : result){
            Map<Integer, Integer> map = new HashMap<>();
            int userId = (int) row.get("user_id");
            int skuId = (int) row.get("sku_id");
            Date date = (Date) row.get("action_date");

            String actionSql = "select model_id from action_1 where user_id=" + userId + " and sku_id=" + skuId + " and date='" + date + "'";
            List<Map<String, Object>> actionResult = DBOperation.queryBySql(actionSql);
            for(Map<String, Object> actionRow : actionResult){
                int modelId = (int) actionRow.get("model_id");
                if(modelId > 0){
                    if(!map.containsKey(modelId)){
                        map.put(modelId, 0);
                    }

                    map.put(modelId, map.get(modelId) + 1);
                }
            }

            if(map.size() > 0){
                JSONObject json = new JSONObject();
                for(Map.Entry<Integer, Integer> entry : map.entrySet()){
                    json.put(String.valueOf(entry.getKey()), entry.getValue());
                }

                String str = json.toJSONString();
                String updateSql = "update user_action_1 set model_id='" + str + "' where user_id=" + userId + " and sku_id=" + skuId + " and action_date='" + date + "'";
                log.info(updateSql);
                DBOperation.update(updateSql);
            }
        }
    }
}

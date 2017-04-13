package jingdong.contest.horizon;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by wanghl on 17-4-13.
 */
public class ActionValidation {
    private static final Logger log = Logger.getLogger(ActionValidation.class);

    public static void main(String[] args) {
        Set<String> actionSet = new HashSet<>();
        Set<String> userActionSet = new HashSet<>();

        String sql = "select user_id,sku_id from action_1 where date>='2016-02-01' group by user_id,sku_id";
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        for(Map<String, Object> row : result){
            int userId = (int) row.get("user_id");
            int productId = (int) row.get("sku_id");
            actionSet.add(userId + "_" + productId);
        }

        sql = "select user_id,sku_id from user_action_horizon";
        result = DBOperation.queryBySql(sql);
        for(Map<String, Object> row : result){
            int userId = (int) row.get("user_id");
            int productId = (int) row.get("sku_id");
            userActionSet.add(userId + "_" + productId);
        }

        log.info("action: " + actionSet.size());
        log.info("user action: " + userActionSet.size());

        actionSet.removeAll(userActionSet);
        for(String s : actionSet){
            log.info(s.split("_")[0] + ": " + s.split("_")[1]);
        }
    }
}

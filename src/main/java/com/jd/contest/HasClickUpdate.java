package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-8.
 */
public class HasClickUpdate {
    private static final Logger log = Logger.getLogger(HasClickUpdate.class);

    public static void main(String[] args) {
        Connection conn = DBConnection.getConnection();
        String sql = "select user_id,sku_id,click from jd_contest.user_action";
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        int i = 0;
        for(Map<String, Object> row : result){
            if(++i % 1000 == 0){
                log.info(i);
            }
            int userId = (int) row.get("user_id");
            int skuId = (int) row.get("sku_id");
            int click = (int) row.get("click");
            if(click > 0){
                String updateSql = "update jd_contest.user_action set has_click=1 where user_id=" + userId + " and sku_id=" + skuId;
                DBOperation.update(conn, updateSql);
            }
        }

        DBConnection.close(conn);
    }
}

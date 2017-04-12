package jingdong.contest.horizon;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-12.
 */
public class Validation {
    private static final Logger log = Logger.getLogger(Validation.class);

    public static void main(String[] args) {
        int totalClick = 0, totalDetail = 0, totalCart = 0, totalCartDelete = 0, totalBuy = 0, totalFollow = 0;
        Connection conn = DBConnection.getConnection();
        /*for(int i = 1; i <= 75; i++){
            String sql = "select sum(click_" + i + ") as click from user_action_horizon";
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            if(result.size() > 0){
                BigDecimal bd = (BigDecimal) result.get(0).get("click");
                totalClick += bd.longValue();
            }
        }

        for(int i = 1; i <= 75; i++){
            String sql = "select sum(detail_" + i + ") as detail from user_action_horizon";
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            if(result.size() > 0){
                BigDecimal bd = (BigDecimal) result.get(0).get("detail");
                totalDetail += bd.longValue();
            }
        }

        for(int i = 1; i <= 75; i++){
            String sql = "select sum(cart_" + i + ") as cart from user_action_horizon";
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            if(result.size() > 0){
                BigDecimal bd = (BigDecimal) result.get(0).get("cart");
                totalCart += bd.longValue();
            }
        }

        for(int i = 1; i <= 75; i++){
            String sql = "select sum(cart_delete_" + i + ") as cart_delete from user_action_horizon";
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            if(result.size() > 0){
                BigDecimal bd = (BigDecimal) result.get(0).get("cart_delete");
                totalCartDelete += bd.longValue();
            }
        }

        for(int i = 1; i <= 75; i++){
            String sql = "select sum(buy_" + i + ") as buy from user_action_horizon";
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            if(result.size() > 0){
                BigDecimal bd = (BigDecimal) result.get(0).get("buy");
                totalBuy += bd.longValue();
            }
        }*/

        for(int i = 1; i <= 75; i++){
            String sql = "select sum(follow_" + i + ") as follow from user_action_horizon";
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            if(result.size() > 0){
                BigDecimal bd = (BigDecimal) result.get(0).get("follow");
                log.info(bd.longValue());
                totalFollow += bd.longValue();
            }
        }

//        System.out.println(totalClick);
//        System.out.println(totalDetail);
//        System.out.println(totalCart);
//        System.out.println(totalCartDelete);
//        System.out.println(totalBuy);
        System.out.println(totalFollow);

        DBConnection.close(conn);
    }
}

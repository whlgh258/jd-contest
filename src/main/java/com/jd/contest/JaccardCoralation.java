package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static javafx.scene.input.KeyCode.M;

/**
 * Created by wanghl on 17-4-8.
 */
public class JaccardCoralation {
    public static void main(String[] args) {
        String sql = "select user_id,sku_id,has_click,has_detail,has_cart,has_cart_delete,has_buy,has_follow,buy_again from jd_contest.user_action";
        Connection conn = DBConnection.getConnection();
        List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
        Map<String, Integer> clickBuyUnion = new HashMap<>();
        Map<String, Integer> clickBuyInsection = new HashMap<>();
        Map<String, Integer> detailBuyUnion = new HashMap<>();
        Map<String, Integer> detailBuyInsection = new HashMap<>();
        Map<String, Integer> cartBuyUnion = new HashMap<>();
        Map<String, Integer> cartBuyInsection = new HashMap<>();
        Map<String, Integer> cartDeleteBuyUnion = new HashMap<>();
        Map<String, Integer> cartDeleteBuyInsection = new HashMap<>();
        Map<String, Integer> followBuyUnion = new HashMap<>();
        Map<String, Integer> followBuyInsection = new HashMap<>();
        Map<String, Integer> clickBuyAgainUnion = new HashMap<>();
        Map<String, Integer> clickBuyAgainInsection = new HashMap<>();
        Map<String, Integer> detailBuyAgainUnion = new HashMap<>();
        Map<String, Integer> detailBuyAgainInsection = new HashMap<>();
        Map<String, Integer> cartBuyAgainUnion = new HashMap<>();
        Map<String, Integer> cartBuyAgainInsection = new HashMap<>();
        Map<String, Integer> cartDeleteBuyAgainUnion = new HashMap<>();
        Map<String, Integer> cartDeleteBuyAgainInsection = new HashMap<>();
        Map<String, Integer> buyBuyAgainUnion = new HashMap<>();
        Map<String, Integer> buyBuyAgainInsection = new HashMap<>();
        Map<String, Integer> followBuyAgainUnion = new HashMap<>();
        Map<String, Integer> followBuyAgainInsection = new HashMap<>();

        for(Map<String, Object> row : result){
            int userId = (int) row.get("user_id");
            int skuId = (int) row.get("sku_id");
            int hasClick = (int) row.get("has_click");
            int hasDetail = (int) row.get("has_detail");
            int hasCart = (int) row.get("has_cart");
            int hasCartDelete = (int) row.get("has_cart_delete");
            int hasBuy = (int) row.get("has_buy");
            int hasFollow = (int) row.get("has_follow");
            int buyAgain = (int) row.get("buy_again");

            String key = userId + "_" + skuId;
            if(hasClick > 0 || hasBuy > 0){
                clickBuyUnion.put(key, 1);
            }
            if(hasClick > 0 && hasBuy > 0){
                clickBuyInsection.put(key, 1);
            }

            if(hasDetail > 0 || hasBuy > 0){
                detailBuyUnion.put(key, 1);
            }
            if(hasDetail > 0 && hasBuy > 0){
                detailBuyInsection.put(key, 1);
            }

            if(hasCart > 0 || hasBuy > 0){
                cartBuyUnion.put(key, 1);
            }
            if(hasCart > 0 && hasBuy > 0){
                cartBuyInsection.put(key, 1);
            }

            if(hasCartDelete > 0 || hasBuy > 0){
                cartDeleteBuyUnion.put(key, 1);
            }
            if(hasCartDelete > 0 && hasBuy > 0){
                cartDeleteBuyInsection.put(key, 1);
            }

            if(hasFollow > 0 || hasBuy > 0){
                followBuyUnion.put(key, 1);
            }
            if(hasFollow > 0 && hasBuy > 0){
                followBuyInsection.put(key, 1);
            }

            if(hasClick > 0 || buyAgain > 0){
                clickBuyAgainUnion.put(key, 1);
            }
            if(hasClick > 0 && buyAgain > 0){
                clickBuyAgainInsection.put(key, 1);
            }

            if(hasDetail > 0 || buyAgain > 0){
                detailBuyAgainUnion.put(key, 1);
            }
            if(hasDetail > 0 && buyAgain > 0){
                detailBuyAgainInsection.put(key, 1);
            }

            if(hasCart > 0 || buyAgain > 0){
                cartBuyAgainUnion.put(key, 1);
            }
            if(hasCart > 0 && buyAgain > 0){
                cartBuyAgainInsection.put(key, 1);
            }

            if(hasCartDelete > 0 || buyAgain > 0){
                cartDeleteBuyAgainUnion.put(key, 1);
            }
            if(hasCartDelete > 0 && buyAgain > 0){
                cartDeleteBuyAgainInsection.put(key, 1);
            }

            if(hasBuy > 0 || buyAgain > 0){
                buyBuyAgainUnion.put(key, 1);
            }
            if(hasBuy > 0 && buyAgain > 0){
                buyBuyAgainInsection.put(key, 1);
            }

            if(hasFollow > 0 || buyAgain > 0){
                followBuyAgainUnion.put(key, 1);
            }
            if(hasFollow > 0 && buyAgain > 0){
                followBuyAgainInsection.put(key, 1);
            }
        }

        System.out.println(clickBuyInsection.size() * 1.0d / clickBuyUnion.size());
        System.out.println(detailBuyInsection.size() * 1.0d / detailBuyUnion.size());
        System.out.println(cartBuyInsection.size() * 1.0d / cartBuyUnion.size());
        System.out.println(cartDeleteBuyInsection.size() * 1.0d / cartDeleteBuyUnion.size());
        System.out.println(followBuyInsection.size() * 1.0d / followBuyUnion.size());
        System.out.println(clickBuyAgainInsection.size() * 1.0d / clickBuyUnion.size());
        System.out.println(detailBuyAgainInsection.size() * 1.0d / detailBuyAgainUnion.size());
        System.out.println(cartBuyAgainInsection.size() * 1.0d / cartBuyAgainUnion.size());
        System.out.println(cartDeleteBuyAgainInsection.size() * 1.0d / cartDeleteBuyAgainUnion.size());
        System.out.println(buyBuyAgainInsection.size() * 1.0d / buyBuyAgainUnion.size());
        System.out.println(followBuyAgainInsection.size() * 1.0d / followBuyAgainUnion.size());
    }
}

import org.apache.log4j.Logger;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-4.
 */
public class ActionAnalysis {
    private static Logger log = Logger.getLogger(ActionAnalysis.class);
    private static final List<Integer> productIds;

    static{
        String productSql = "select distinct sku_id from jd.action order by sku_id";
        List<Map<String, Object>> productResult = DBOperation.queryBySql(productSql);
        productIds = new ArrayList<>();
        for(Map<String, Object> productRow : productResult){
            productIds.add((int) productRow.get("sku_id"));
        }

        log.info("product size: " + productIds.size());
    }

    public static void main(String[] args) {
        Connection conn = DBConnection.getConnection();
        String userSql = "select distinct user_id from jd.action order by user_id";
        List<Map<String, Object>> userResult = DBOperation.queryBySql(conn, userSql);
        log.info("uers size: " + userResult.size());
        int i = 0;
        int j = 0;
        for(Map<String, Object> userRow : userResult){
            int userId = (int) userRow.get("user_id");
            log.info(++j);
            List<Map<String, Object>> insertList = new ArrayList<>();
            for(int productId : productIds){
                String clickSql = "select count(1) as count from jd.action where type=6 and user_id=" + userId + " and sku_id=" + productId;
                List<Map<String, Object>> clickResult = DBOperation.queryBySql(conn, clickSql);
                if(0 == clickResult.size()){
                    continue;
                }

                Map<String, Object> clickRow = clickResult.get(0);
                long click = (long) clickRow.get("count");
                if(0 == click){
                    continue;
                }

                ++i;

                String detailSql = "select count(1) as count from jd.action where type=1 and user_id=" + userId + " and sku_id=" + productId;
                List<Map<String, Object>> detailResult = DBOperation.queryBySql(conn, detailSql);

                long detail = 0;
                if(detailResult.size() > 0){
                    Map<String, Object> detailRow = detailResult.get(0);
                    detail = (long) detailRow.get("count");
                }

                String cartSql = "select count(1) as count from jd.action where type=2 and user_id=" + userId + " and sku_id=" + productId;
                List<Map<String, Object>> cartResult = DBOperation.queryBySql(conn, cartSql);

                long cart = 0;
                if(cartResult.size() > 0){
                    Map<String, Object> cartRow = cartResult.get(0);
                    cart = (long) cartRow.get("count");
                }

                String cartDeleteSql = "select count(1) as count from jd.action where type=3 and user_id=" + userId + " and sku_id=" + productId;
                List<Map<String, Object>> cartDeleteResult = DBOperation.queryBySql(conn, cartDeleteSql);

                long cartDelete = 0;
                if(cartDeleteResult.size() > 0){
                    Map<String, Object> cartDeleteRow = cartDeleteResult.get(0);
                    cartDelete = (long) cartDeleteRow.get("count");
                }

                String buySql = "select count(1) as count from jd.action where type=4 and user_id=" + userId + " and sku_id=" + productId;
                List<Map<String, Object>> buyResult = DBOperation.queryBySql(conn, buySql);

                long buy = 0;
                if(buyResult.size() > 0){
                    Map<String, Object> buyRow = buyResult.get(0);
                    buy = (long) buyRow.get("count");
                }

                String followSql = "select count(1) as count from jd.action where type=5 and user_id=" + userId + " and sku_id=" + productId;
                List<Map<String, Object>> followResult = DBOperation.queryBySql(conn, followSql);

                long follow = 0;
                if(followResult.size() > 0){
                    Map<String, Object> followRow = followResult.get(0);
                    follow = (long) followRow.get("count");
                }

                Map<String, Object> insertMap = new HashMap<>();
                insertMap.put("user_id", userId);
                insertMap.put("sku_id", productId);
                insertMap.put("click", click);
                insertMap.put("detail", detail);
                insertMap.put("cart", cart);
                insertMap.put("cart_delete", cartDelete);
                insertMap.put("buy", buy);
                insertMap.put("follow", follow);

                insertList.add(insertMap);
            }

//            DBOperation.insert(conn, "jd.user_action", insertList);
        }

        System.out.println(i);
    }
}











































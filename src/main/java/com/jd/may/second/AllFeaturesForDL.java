package com.jd.may.second;

import au.com.bytecode.opencsv.CSVWriter;
import com.alibaba.fastjson.JSONObject;
import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.util.*;

/**
 * Created by wanghl on 17-5-14.
 */
public class AllFeaturesForDL {
    private static final Logger log = Logger.getLogger(AllFeaturesForDL.class);

    private static final String path = "/home/wanghl/jd_contest/0513/";
    private static final int[] modelIds = new int[]{11,217,27,216,17,210,14,211,218,26,28,24,220,21,25,15,221,222,29,23,19,16,223,219,116,224,22,31,125,13,111,18,33,119,333,112,322,311,318,110,319,34,12,124,120,121,325,331,329,334,346,328,32,320,341,348,340,323,337,343,345,336,312,321,335,347,344,342,316,315,36,115,114,113,122,317,212,313,35,314,39,338,225,310,324,332,327,37,38,330};
    private static final Map<String, Integer> attributes;

    static {
        attributes = new LinkedHashMap<>();
        int i = 0;
        attributes.put("user_id", i++);
        attributes.put("sku_id", i++);
        attributes.put("age", i++);
        attributes.put("sex", i++);
        attributes.put("user_level", i++);
        attributes.put("attr1", i++);
        attributes.put("attr2", i++);
        attributes.put("attr3", i++);
        attributes.put("cate", i++);
        attributes.put("brand", i++);
        attributes.put("comment_num", i++);
        attributes.put("has_bad_comment", i++);
        attributes.put("bad_comment_rate", i++);
        for(int modelId : modelIds){
            attributes.put("model_" + modelId, i++);
        }

        attributes.put("user_click_count", i++);
        attributes.put("user_detail_count", i++);
        attributes.put("user_cart_count", i++);
        attributes.put("user_cart_delete_count", i++);
        attributes.put("user_buy_count", i++);
        attributes.put("user_follow_count", i++);
        attributes.put("user_click_sum", i++);
        attributes.put("user_detail_sum", i++);
        attributes.put("user_cart_sum", i++);
        attributes.put("user_cart_delete_sum", i++);
        attributes.put("user_buy_sum", i++);
        attributes.put("user_follow_sum", i++);
        attributes.put("user_click_avg", i++);
        attributes.put("user_detail_avg", i++);
        attributes.put("user_cart_avg", i++);
        attributes.put("user_cart_delete_avg", i++);
        attributes.put("user_buy_avg", i++);
        attributes.put("user_follow_avg", i++);
        attributes.put("user_buy_click_ratio", i++);
        attributes.put("user_buy_detail_ratio", i++);
        attributes.put("user_buy_cart_ratio", i++);
        attributes.put("user_buy_cart_delete_ratio", i++);
        attributes.put("user_buy_follow_ratio", i++);

        attributes.put("item_click_count", i++);
        attributes.put("item_detail_count", i++);
        attributes.put("item_cart_count", i++);
        attributes.put("item_cart_delete_count", i++);
        attributes.put("item_buy_count", i++);
        attributes.put("item_follow_count", i++);
        attributes.put("item_click_sum", i++);
        attributes.put("item_detail_sum", i++);
        attributes.put("item_cart_sum", i++);
        attributes.put("item_cart_delete_sum", i++);
        attributes.put("item_buy_sum", i++);
        attributes.put("item_follow_sum", i++);
        attributes.put("item_click_avg", i++);
        attributes.put("item_detail_avg", i++);
        attributes.put("item_cart_avg", i++);
        attributes.put("item_cart_delete_avg", i++);
        attributes.put("item_buy_avg", i++);
        attributes.put("item_follow_avg", i++);
        attributes.put("item_buy_click_ratio", i++);
        attributes.put("item_buy_detail_ratio", i++);
        attributes.put("item_buy_cart_ratio", i++);
        attributes.put("item_buy_cart_delete_ratio", i++);
        attributes.put("item_buy_follow_ratio", i++);

        attributes.put("user_item_click_count", i++);
        attributes.put("user_item_detail_count", i++);
        attributes.put("user_item_cart_count", i++);
        attributes.put("user_item_cart_delete_count", i++);
        attributes.put("user_item_buy_count", i++);
        attributes.put("user_item_follow_count", i++);
        attributes.put("user_item_click_sum", i++);
        attributes.put("user_item_detail_sum", i++);
        attributes.put("user_item_cart_sum", i++);
        attributes.put("user_item_cart_delete_sum", i++);
        attributes.put("user_item_buy_sum", i++);
        attributes.put("user_item_follow_sum", i++);
        attributes.put("user_item_click_avg", i++);
        attributes.put("user_item_detail_avg", i++);
        attributes.put("user_item_cart_avg", i++);
        attributes.put("user_item_cart_delete_avg", i++);
        attributes.put("user_item_buy_avg", i++);
        attributes.put("user_item_follow_avg", i++);
        attributes.put("user_item_buy_click_ratio", i++);
        attributes.put("user_item_buy_detail_ratio", i++);
        attributes.put("user_item_buy_cart_ratio", i++);
        attributes.put("user_item_buy_cart_delete_ratio", i++);
        attributes.put("user_item_buy_follow_ratio", i++);

        attributes.put("user_popular", i++);
        attributes.put("item_popular", i++);

        attributes.put("item_action_user", i++);

        attributes.put("user_is_buy", i++);
        attributes.put("item_is_buy", i++);
        attributes.put("item_buy_dates", i++);

        attributes.put("user_distinct_item_click", i++);
        attributes.put("user_distinct_item_detail", i++);
        attributes.put("user_distinct_item_cart", i++);
        attributes.put("user_distinct_item_cart_delete", i++);
        attributes.put("user_distinct_item_buy", i++);
        attributes.put("user_distinct_item_follow", i++);

        attributes.put("item_distinct_user_click", i++);
        attributes.put("item_distinct_user_detail", i++);
        attributes.put("item_distinct_user_cart", i++);
        attributes.put("item_distinct_user_cart_delete", i++);
        attributes.put("item_distinct_user_buy", i++);
        attributes.put("item_distinct_user_follow", i++);

        attributes.put("user_ratio_user_click_cross", i++);
        attributes.put("user_ratio_user_detail_cross", i++);
        attributes.put("user_ratio_user_cart_cross", i++);
        attributes.put("user_ratio_user_cart_delete_cross", i++);
        attributes.put("user_ratio_user_follow_cross", i++);
        attributes.put("user_ratio_user_item_click_cross", i++);
        attributes.put("user_ratio_user_item_detail_cross", i++);
        attributes.put("user_ratio_user_item_cart_cross", i++);
        attributes.put("user_ratio_user_item_cart_delete_cross", i++);
        attributes.put("user_ratio_user_item_follow_cross", i++);

        attributes.put("item_ratio_item_click_cross", i++);
        attributes.put("item_ratio_item_detail_cross", i++);
        attributes.put("item_ratio_item_cart_cross", i++);
        attributes.put("item_ratio_item_cart_delete_cross", i++);
        attributes.put("item_ratio_item_follow_cross", i++);
        attributes.put("item_ratio_user_item_click_cross", i++);
        attributes.put("item_ratio_user_item_detail_cross", i++);
        attributes.put("item_ratio_user_item_cart_cross", i++);
        attributes.put("item_ratio_user_item_cart_delete_cross", i++);
        attributes.put("item_ratio_user_item_follow_cross", i++);

        attributes.put("user_item_ratio_user_click_cross", i++);
        attributes.put("user_item_ratio_user_detail_cross", i++);
        attributes.put("user_item_ratio_user_cart_cross", i++);
        attributes.put("user_item_ratio_user_cart_delete_cross", i++);
        attributes.put("user_item_ratio_user_follow_cross", i++);
        attributes.put("user_item_ratio_item_click_cross", i++);
        attributes.put("user_item_ratio_item_detail_cross", i++);
        attributes.put("user_item_ratio_item_cart_cross", i++);
        attributes.put("user_item_ratio_item_cart_delete_cross", i++);
        attributes.put("user_item_ratio_item_follow_cross", i++);
        attributes.put("user_item_ratio_user_item_click_cross", i++);
        attributes.put("user_item_ratio_user_item_detail_cross", i++);
        attributes.put("user_item_ratio_user_item_cart_cross", i++);
        attributes.put("user_item_ratio_user_item_cart_delete_cross", i++);
        attributes.put("user_item_ratio_user_item_follow_cross", i++);

        attributes.put("target", i++);
        log.info("feature size: " + attributes.size());
    }

    public static List<String[]> features(String start, String end, String labelStart, String labelEnd, boolean isPredict, String filename) throws Exception{
        List<String[]> lines = new ArrayList<>();

        Map<String,Map<String, Object>> userInfos = new HashMap<>();
        String userInfoSql = "select user_id,sku_id,max(age) as age,max(sex) as sex,max(user_level) as user_level,max(attr1) as attr1,max(attr2) as attr2,max(attr3) as attr3,max(cate) as cate,max(brand) as brand,max(comment_num) as comment_num,max(has_bad_comment) as has_bad_comment,max(bad_comment_rate) as bad_comment_rate from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        log.info("user info sql: " + userInfoSql);
        List<Map<String, Object>> result = DBOperation.queryBySql(userInfoSql);
        log.info("user info size: " + result.size());
        for(Map<String, Object> row : result){
            String userId = String.valueOf(row.get("user_id"));
            String skuId = String.valueOf(row.get("sku_id"));
            userInfos.put(userId + "_" + skuId, row);
        }

        String userCountSql = "select user_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String userSumSql = "select user_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String userAvgSql = "select user_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(buy)+1),3) as buy,round(log(avg(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String userBuyRatioSql = "select user_id,round(if(sum(click)>0,log(sum(buy)+1)/log(sum(click)+1),0),3) as click,round(if(sum(detail)>0,log(sum(buy)+1)/log(sum(detail)+1),0),3) as detail,round(if(sum(cart)>0,log(sum(buy)+1)/log(sum(cart)+1),0),3) as cart,round(if(sum(cart_delete)>0,log(sum(buy)+1)/log(sum(cart_delete)+1),0),3) as cart_delete,round(if(sum(buy>0),log(sum(buy)+1)/log(sum(buy)+1),0),3) as buy,round(if(sum(follow)>0,log(sum(buy)+1)/log(sum(follow)+1),0),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String itemCountSql = "select sku_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String itemSumSql = "select sku_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String itemAvgSql = "select sku_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(buy)+1),3) as buy,round(log(avg(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String itemBuyRatioSql = "select sku_id,round(if(sum(click)>0,log(sum(buy)+1)/log(sum(click)+1),0),3) as click,round(if(sum(detail)>0,log(sum(buy)+1)/log(sum(detail)+1),0),3) as detail,round(if(sum(cart)>0,log(sum(buy)+1)/log(sum(cart)+1),0),3) as cart,round(if(sum(cart_delete)>0,log(sum(buy)+1)/log(sum(cart_delete)+1),0),3) as cart_delete,round(if(sum(buy>0),log(sum(buy)+1)/log(sum(buy)+1),0),3) as buy,round(if(sum(follow)>0,log(sum(buy)+1)/log(sum(follow)+1),0),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String userItemCountSql = "select user_id,sku_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        String userItemSumSql = "select user_id,sku_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        String userItemAvgSql = "select user_id,sku_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(buy)+1),3) as buy,round(log(avg(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        String userItemBuyRatioSql = "select user_id,sku_id,round(if(sum(click)>0,log(sum(buy)+1)/log(sum(click)+1),0),3) as click,round(if(sum(detail)>0,log(sum(buy)+1)/log(sum(detail)+1),0),3) as detail,round(if(sum(cart)>0,log(sum(buy)+1)/log(sum(cart)+1),0),3) as cart,round(if(sum(cart_delete)>0,log(sum(buy)+1)/log(sum(cart_delete)+1),0),3) as cart_delete,round(if(sum(buy>0),log(sum(buy)+1)/log(sum(buy)+1),0),3) as buy,round(if(sum(follow)>0,log(sum(buy)+1)/log(sum(follow)+1),0),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";

        Map<String, Map<String, Double>> userCountFeature = Features.countFeature(userCountSql, 0);
        Map<String, Map<String, Double>> userSumFeature = Features.sumFeature(userSumSql, 0);
        Map<String, Map<String, Double>> userAvgFeature = Features.avgFeature(userAvgSql, 0);
        Map<String, Map<String, Double>> userBuyRatioFeature = Features.buyRatioFeature(userBuyRatioSql, 0);
        Map<String, Map<String, Double>> itemCountFeature = Features.countFeature(itemCountSql, 1);
        Map<String, Map<String, Double>> itemSumFeature = Features.sumFeature(itemSumSql, 1);
        Map<String, Map<String, Double>> itemAvgFeature = Features.avgFeature(itemAvgSql, 1);
        Map<String, Map<String, Double>> itemBuyRatioFeature = Features.buyRatioFeature(itemBuyRatioSql, 1);
        Map<String, Map<String, Double>> userItemCountFeature = Features.countFeature(userItemCountSql, 2);
        Map<String, Map<String, Double>> userItemSumFeature = Features.sumFeature(userItemSumSql, 2);
        Map<String, Map<String, Map<String, Double>>> userItemSumCrossFeature = Features.sumFeatureForCross(userItemSumSql);
        Map<String, Map<String, Double>> userItemAvgFeature = Features.avgFeature(userItemAvgSql, 2);
        Map<String, Map<String, Double>> userItemBuyRatioFeature = Features.buyRatioFeature(userItemBuyRatioSql, 2);

        Map<String, Double> userPopular = Popular.userPopular(start, end);
        Map<String, Double> itemPopular = Popular.itemPopular(start, end);


        Map<String, Double> itemActionUserCount = Popular.itemActionUserCount(start, end);

        Map<String, Integer> userIsBuy = ItemBuyDays.userIsBuy(start, end);
        Map<String, Integer> itemIsBuy = ItemBuyDays.itemIsBuy(start, end);
        Map<String, Integer> itemBuyDate = ItemBuyDays.itemBuyDays(start, end);

        String userItemClickSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where click>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        Map<String, Double> userItemClick = ItemBuyUsers.itemUserCount(userItemClickSql, "user_id");
        String userItemDetailSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where detail>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        Map<String, Double> userItemDetail = ItemBuyUsers.itemUserCount(userItemDetailSql, "user_id");
        String userItemCartSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where cart>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        Map<String, Double> userItemCart = ItemBuyUsers.itemUserCount(userItemCartSql, "user_id");
        String userItemCartDeleteSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where cart_delete>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        Map<String, Double> userItemCartDelete = ItemBuyUsers.itemUserCount(userItemCartDeleteSql, "user_id");
        String userItemBuySql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where buy>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        Map<String, Double> userItemBuy = ItemBuyUsers.itemUserCount(userItemBuySql, "user_id");
        String userItemFollowSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where follow>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        Map<String, Double> userItemFollow = ItemBuyUsers.itemUserCount(userItemFollowSql, "user_id");

        String itemUserClickSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where click>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserClick = ItemBuyUsers.itemUserCount(itemUserClickSql, "sku_id");
        String itemUserDetailSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where detail>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserDetail = ItemBuyUsers.itemUserCount(itemUserDetailSql, "sku_id");
        String itemUserCartSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where cart>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserCart = ItemBuyUsers.itemUserCount(itemUserCartSql, "sku_id");
        String itemUserCartDeleteSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where cart_delete>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserCartDelete = ItemBuyUsers.itemUserCount(itemUserCartDeleteSql, "sku_id");
        String itemUserBuySql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where buy>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserBuy = ItemBuyUsers.itemUserCount(itemUserBuySql, "sku_id");
        String itemUserFollowSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where follow>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserFollow = ItemBuyUsers.itemUserCount(itemUserFollowSql, "sku_id");

        // 交叉特征 user-ratio
        Map<String, Double> userRatioUserClickCross = new HashMap<>();
        Map<String, Double> userRatioUserDetailCross = new HashMap<>();
        Map<String, Double> userRatioUserCartCross = new HashMap<>();
        Map<String, Double> userRatioUserCartDeleteCross = new HashMap<>();
        Map<String, Double> userRatioUserFollowCross = new HashMap<>();

        Map<String, Double> userRatioUserItemClickCross = new HashMap<>();
        Map<String, Double> userRatioUserItemDetailCross = new HashMap<>();
        Map<String, Double> userRatioUserItemCartCross = new HashMap<>();
        Map<String, Double> userRatioUserItemCartDeleteCross = new HashMap<>();
        Map<String, Double> userRatioUserItemFollowCross = new HashMap<>();

        // item-ratio
        Map<String, Double> itemRatioItemClickCross = new HashMap<>();
        Map<String, Double> itemRatioItemDetailCross = new HashMap<>();
        Map<String, Double> itemRatioItemCartCross = new HashMap<>();
        Map<String, Double> itemRatioItemCartDeleteCross = new HashMap<>();
        Map<String, Double> itemRatioItemFollowCross = new HashMap<>();

        Map<String, Double> itemRatioUserItemClickCross = new HashMap<>();
        Map<String, Double> itemRatioUserItemDetailCross = new HashMap<>();
        Map<String, Double> itemRatioUserItemCartCross = new HashMap<>();
        Map<String, Double> itemRatioUserItemCartDeleteCross = new HashMap<>();
        Map<String, Double> itemRatioUserItemFollowCross = new HashMap<>();

        // user-item ratio
        Map<String, Double> userItemRatioUserClickCross = new HashMap<>();
        Map<String, Double> userItemRatioUserDetailCross = new HashMap<>();
        Map<String, Double> userItemRatioUserCartCross = new HashMap<>();
        Map<String, Double> userItemRatioUserCartDeleteCross = new HashMap<>();
        Map<String, Double> userItemRatioUserFollowCross = new HashMap<>();

        Map<String, Double> userItemRatioItemClickCross = new HashMap<>();
        Map<String, Double> userItemRatioItemDetailCross = new HashMap<>();
        Map<String, Double> userItemRatioItemCartCross = new HashMap<>();
        Map<String, Double> userItemRatioItemCartDeleteCross = new HashMap<>();
        Map<String, Double> userItemRatioItemFollowCross = new HashMap<>();

        Map<String, Double> userItemRatioUserItemClickCross = new HashMap<>();
        Map<String, Double> userItemRatioUserItemDetailCross = new HashMap<>();
        Map<String, Double> userItemRatioUserItemCartCross = new HashMap<>();
        Map<String, Double> userItemRatioUserItemCartDeleteCross = new HashMap<>();
        Map<String, Double> userItemRatioUserItemFollowCross = new HashMap<>();

        for(Map.Entry<String, Map<String, Double>> entry : userBuyRatioFeature.entrySet()){
            String userId = entry.getKey();
            Map<String, Double> value = entry.getValue();
            double buyClickRatio = value.get("click");
            double buyDetailRatio = value.get("detail");
            double buyCartRatio = value.get("cart");
            double buyCartDeleteRatio = value.get("cartDelete");
            double buyFollowRatio = value.get("follow");

            userRatioUserClickCross.put(userId, DigitalFormat.formatForDouble(buyClickRatio * (null == userSumFeature.get(userId).get("click") ? 0 : DigitalFormat.formatForDouble(userSumFeature.get(userId).get("click")))));
            userRatioUserDetailCross.put(userId, DigitalFormat.formatForDouble(buyDetailRatio * (null == userSumFeature.get(userId).get("detail") ? 0 : userSumFeature.get(userId).get("detail"))));
            userRatioUserCartCross.put(userId, DigitalFormat.formatForDouble(buyCartRatio * (null == userSumFeature.get(userId).get("cart") ? 0 : userSumFeature.get(userId).get("cart"))));
            userRatioUserCartDeleteCross.put(userId, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == userSumFeature.get(userId).get("cartDelete") ? 0 : userSumFeature.get(userId).get("cartDelete"))));
            userRatioUserFollowCross.put(userId, DigitalFormat.formatForDouble(buyFollowRatio * (null == userSumFeature.get(userId).get("follow") ? 0 : userSumFeature.get(userId).get("follow"))));

            Map<String, Map<String, Double>> userMap = userItemSumCrossFeature.get(userId);
            for(Map.Entry<String, Map<String, Double>> ent : userMap.entrySet()){
                String key = ent.getKey();
                Map<String, Double> sumMap = ent.getValue();
                userRatioUserItemClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == sumMap.get("click") ? 0 : sumMap.get("click"))));
                userRatioUserItemDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == sumMap.get("detail") ? 0 : sumMap.get("detail"))));
                userRatioUserItemCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == sumMap.get("cart") ? 0 : sumMap.get("cart"))));
                userRatioUserItemCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == sumMap.get("cartDelete") ? 0 : sumMap.get("cartDelete"))));
                userRatioUserItemFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == sumMap.get("follow") ? 0 : sumMap.get("follow"))));
            }
        }

        for(Map.Entry<String, Map<String, Double>> entry : itemBuyRatioFeature.entrySet()){
            String skuId = entry.getKey();
            Map<String, Double> value = entry.getValue();
            double buyClickRatio = value.get("click");
            double buyDetailRatio = value.get("detail");
            double buyCartRatio = value.get("cart");
            double buyCartDeleteRatio = value.get("cartDelete");
            double buyFollowRatio = value.get("follow");

            itemRatioItemClickCross.put(skuId, DigitalFormat.formatForDouble(buyClickRatio * (null == itemSumFeature.get(skuId).get("click") ? 0 : itemSumFeature.get(skuId).get("click"))));
            itemRatioItemDetailCross.put(skuId, DigitalFormat.formatForDouble(buyDetailRatio * (null == itemSumFeature.get(skuId).get("detail") ? 0 : itemSumFeature.get(skuId).get("detail"))));
            itemRatioItemCartCross.put(skuId, DigitalFormat.formatForDouble(buyCartRatio * (null == itemSumFeature.get(skuId).get("cart") ? 0 : itemSumFeature.get(skuId).get("cart"))));
            itemRatioItemCartDeleteCross.put(skuId, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == itemSumFeature.get(skuId).get("cartDelete") ? 0 : itemSumFeature.get(skuId).get("cartDelete"))));
            itemRatioItemFollowCross.put(skuId, DigitalFormat.formatForDouble(buyFollowRatio * (null == itemSumFeature.get(skuId).get("follow") ? 0 : itemSumFeature.get(skuId).get("follow"))));

            Map<String, Map<String, Double>> itemMap = userItemSumCrossFeature.get(skuId);
            for(Map.Entry<String, Map<String, Double>> ent : itemMap.entrySet()) {
                String key = ent.getKey();
                Map<String, Double> sumMap = ent.getValue();
                itemRatioUserItemClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == sumMap.get("click") ? 0 : sumMap.get("click"))));
                itemRatioUserItemDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == sumMap.get("detail") ? 0 : sumMap.get("detail"))));
                itemRatioUserItemCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == sumMap.get("cart") ? 0 : sumMap.get("cart"))));
                itemRatioUserItemCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == sumMap.get("cartDelete") ? 0 : sumMap.get("cartDelete"))));
                itemRatioUserItemFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == sumMap.get("follow") ? 0 : sumMap.get("follow"))));
            }
        }

        for(Map.Entry<String, Map<String, Double>> entry : userItemBuyRatioFeature.entrySet()){
            String key = entry.getKey();
            String userId = key.split("_")[0];
            String skuId = key.split("_")[1];
            Map<String, Double> value = entry.getValue();
            double buyClickRatio = value.get("click");
            double buyDetailRatio = value.get("detail");
            double buyCartRatio = value.get("cart");
            double buyCartDeleteRatio = value.get("cartDelete");
            double buyFollowRatio = value.get("follow");

            userItemRatioUserClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == userSumFeature.get(userId).get("click") ? 0 : userSumFeature.get(userId).get("click"))));
            userItemRatioUserDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == userSumFeature.get(userId).get("detail") ? 0 : userSumFeature.get(userId).get("detail"))));
            userItemRatioUserCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == userSumFeature.get(userId).get("cart") ? 0 : userSumFeature.get(userId).get("cart"))));
            userItemRatioUserCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == userSumFeature.get(userId).get("cartDelete") ? 0 : userSumFeature.get(userId).get("cartDelete"))));
            userItemRatioUserFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == userSumFeature.get(userId).get("follow") ? 0 : userSumFeature.get(userId).get("follow"))));

            userItemRatioItemClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == itemSumFeature.get(skuId).get("click") ? 0 : itemSumFeature.get(skuId).get("click"))));
            userItemRatioItemDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == itemSumFeature.get(skuId).get("detail") ? 0 : itemSumFeature.get(skuId).get("detail"))));
            userItemRatioItemCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == itemSumFeature.get(skuId).get("cart") ? 0 : itemSumFeature.get(skuId).get("cart"))));
            userItemRatioItemCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == itemSumFeature.get(skuId).get("cartDelete") ? 0 : itemSumFeature.get(skuId).get("cartDelete"))));
            userItemRatioItemFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == itemSumFeature.get(skuId).get("follow") ? 0 : itemSumFeature.get(skuId).get("follow"))));

            userItemRatioUserItemClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == userItemSumFeature.get(key).get("click") ? 0 : userItemSumFeature.get(key).get("click"))));
            userItemRatioUserItemDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == userItemSumFeature.get(key).get("detail") ? 0 : userItemSumFeature.get(key).get("detail"))));
            userItemRatioUserItemCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == userItemSumFeature.get(key).get("cart") ? 0 : userItemSumFeature.get(key).get("cart"))));
            userItemRatioUserItemCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == userItemSumFeature.get(key).get("cartDelete") ? 0 : userItemSumFeature.get(key).get("cartDelete"))));
            userItemRatioUserItemFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == userItemSumFeature.get(key).get("follow") ? 0 : userItemSumFeature.get(key).get("follow"))));
        }

        Map<String, Long> labelMap = new HashMap<>();
        if(!isPredict){
            String labelSql = "select user_id,sku_id,count(1) as count from user_action_1 where buy>0 and action_date>='" + labelStart + "' and action_date<='" + labelEnd + "' group by user_id,sku_id";
            log.info("label sql: " + labelSql);
            List<Map<String, Object>> labelResult = DBOperation.queryBySql(labelSql);
            for(Map<String, Object> row : labelResult){
                String userId = String.valueOf(row.get("user_id"));
                String skuId = String.valueOf(row.get("sku_id"));
                long count = (long) row.get("count");

                labelMap.put(userId + "_" + skuId, count);
            }
        }

        int positive = 0;
        int negative = 0;
        String sql = "select user_id,sku_id,group_concat(model_id separator '_') as model_id from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        log.info("info sql: " + sql);
        List<Map<String, Object>> infoResult = DBOperation.queryBySql(sql);
        for(Map<String, Object> row : infoResult) {
            int length = 0;
            if(isPredict){
                length = attributes.size() - 1;
            }
            else{
                length = attributes.size();
            }

            String[] features = new String[length];
            for(int i = 0; i < length; i++){
                features[i] = "0";
            }

            String userId = String.valueOf(row.get("user_id"));
            String skuId = String.valueOf(row.get("sku_id"));

            Map<String, Object> userInfo = userInfos.get(userId + "_" + skuId);

            int age = (int) userInfo.get("age");
            int sex = (int) userInfo.get("sex");
            int userLevel = (int) userInfo.get("user_level");
            int attr1 = (int) userInfo.get("attr1");
            int attr2 = (int) userInfo.get("attr2");
            int attr3 = (int) userInfo.get("attr3");
            int cate = (int) userInfo.get("cate");
            int brand = (int) userInfo.get("brand");
            int commentNum = (int) userInfo.get("comment_num");
            int hasBadComment = (int) userInfo.get("has_bad_comment");
            double badCommentRate = (double) userInfo.get("bad_comment_rate");

            features[0] = userId;
            features[1] = skuId;
            features[2] = String.valueOf(age);
            features[3] = String.valueOf(sex);
            features[4] = String.valueOf(userLevel);
            features[5] = String.valueOf(attr1);
            features[6] = String.valueOf(attr2);
            features[7] = String.valueOf(attr3);
            features[8] = String.valueOf(cate);
            features[9] = String.valueOf(brand);
            features[10] = String.valueOf(commentNum);
            features[11] = String.valueOf(hasBadComment);
            features[12] = String.valueOf(badCommentRate);

            Map<String, Integer> modelIdMap = new HashMap<>();
            String modelIdStr = (String) row.get("model_id");
            if(StringUtils.isNotBlank(modelIdStr)){
                String[] parts = modelIdStr.split("_");
                for (String part : parts) {
                    if (StringUtils.isNotBlank(part) && !"null".equalsIgnoreCase(part)) {
                        JSONObject json = JSONObject.parseObject(part);
                        for (String key : json.keySet()) {
                            if (!modelIdMap.containsKey(key)) {
                                modelIdMap.put(key, 0);
                            }
                            modelIdMap.put(key, modelIdMap.get(key) + json.getIntValue(key));
                        }
                    }
                }

                for(Map.Entry<String, Integer> entry : modelIdMap.entrySet()){
                    int index = attributes.get("model_" + entry.getKey());
                    features[index] = String.valueOf(entry.getValue());
                }
            }

            setFeature(features, userCountFeature.get(userId), 103, false);
            setFeature(features, userSumFeature.get(userId), 109, false);
            setFeature(features, userAvgFeature.get(userId), 115, false);
            setFeature(features, userBuyRatioFeature.get(userId), 121, true);

            setFeature(features, itemCountFeature.get(skuId), 126, false);
            setFeature(features, itemSumFeature.get(skuId), 132, false);
            setFeature(features, itemAvgFeature.get(skuId), 138, false);
            setFeature(features, itemBuyRatioFeature.get(skuId), 144, true);

            setFeature(features, userItemCountFeature.get(userId + "_" + skuId), 149, false);
            setFeature(features, userItemSumFeature.get(userId + "_" + skuId), 155, false);
            setFeature(features, userItemAvgFeature.get(userId + "_" + skuId), 161, false);
            setFeature(features, userItemBuyRatioFeature.get(userId + "_" + skuId), 167, true);

            features[172] = String.valueOf(null == userPopular.get(userId) ? 0 : userPopular.get(userId));
            features[173] = String.valueOf(null == itemPopular.get(skuId) ? 0 : itemPopular.get(skuId));
            features[174] = String.valueOf(null == itemActionUserCount.get(skuId) ? 0 : itemActionUserCount.get(skuId));

            features[175] = String.valueOf(null == userIsBuy.get(userId) ? 0 : userIsBuy.get(userId));
            features[176] = String.valueOf(null == itemIsBuy.get(skuId) ? 0 : itemIsBuy.get(skuId));
            features[177] = String.valueOf(null == itemBuyDate.get(skuId) ? 0 : itemBuyDate.get(skuId));

            features[178] = String.valueOf(null == userItemClick.get(userId) ? 0 : userItemClick.get(userId));
            features[179] = String.valueOf(null == userItemDetail.get(userId) ? 0 : userItemDetail.get(userId));
            features[180] = String.valueOf(null == userItemCart.get(userId) ? 0 : userItemCart.get(userId));
            features[181] = String.valueOf(null == userItemCartDelete.get(userId) ? 0 : userItemCartDelete.get(userId));
            features[182] = String.valueOf(null == userItemBuy.get(userId) ? 0 : userItemBuy.get(userId));
            features[183] = String.valueOf(null == userItemFollow.get(userId) ? 0 : userItemFollow.get(userId));

            features[184] = String.valueOf(null == itemUserClick.get(skuId) ? 0 : itemUserClick.get(skuId));
            features[185] = String.valueOf(null == itemUserDetail.get(skuId) ? 0 : itemUserDetail.get(skuId));
            features[186] = String.valueOf(null == itemUserCart.get(skuId) ? 0 : itemUserCart.get(skuId));
            features[187] = String.valueOf(null == itemUserCartDelete.get(skuId) ? 0 : itemUserCartDelete.get(skuId));
            features[188] = String.valueOf(null == itemUserBuy.get(skuId) ? 0 : itemUserBuy.get(skuId));
            features[189] = String.valueOf(null == itemUserFollow.get(skuId) ? 0 : itemUserFollow.get(skuId));

            features[190] = String.valueOf(null == userRatioUserClickCross.get(userId) ? 0 : userRatioUserClickCross.get(userId));
            features[191] = String.valueOf(null == userRatioUserDetailCross.get(userId) ? 0 : userRatioUserDetailCross.get(userId));
            features[192] = String.valueOf(null == userRatioUserCartCross.get(userId) ? 0 : userRatioUserCartCross.get(userId));
            features[193] = String.valueOf(null == userRatioUserCartDeleteCross.get(userId) ? 0 : userRatioUserCartDeleteCross.get(userId));
            features[194] = String.valueOf(null == userRatioUserFollowCross.get(userId) ? 0 : userRatioUserFollowCross.get(userId));

            features[195] = String.valueOf(null == userRatioUserItemClickCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemClickCross.get(userId + "_" + skuId));
            features[196] = String.valueOf(null == userRatioUserItemDetailCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemDetailCross.get(userId + "_" + skuId));
            features[197] = String.valueOf(null == userRatioUserItemCartCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemCartCross.get(userId + "_" + skuId));
            features[198] = String.valueOf(null == userRatioUserItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemCartDeleteCross.get(userId + "_" + skuId));
            features[199] = String.valueOf(null == userRatioUserItemFollowCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemFollowCross.get(userId + "_" + skuId));

            features[200] = String.valueOf(null == itemRatioItemClickCross.get(skuId) ? 0 : itemRatioItemClickCross.get(skuId));
            features[201] = String.valueOf(null == itemRatioItemDetailCross.get(skuId) ? 0 : itemRatioItemDetailCross.get(skuId));
            features[202] = String.valueOf(null == itemRatioItemCartCross.get(skuId) ? 0 : itemRatioItemCartCross.get(skuId));
            features[203] = String.valueOf(null == itemRatioItemCartDeleteCross.get(skuId) ? 0 : itemRatioItemCartDeleteCross.get(skuId));
            features[204] = String.valueOf(null == itemRatioItemFollowCross.get(skuId) ? 0 : itemRatioItemFollowCross.get(skuId));

            features[205] = String.valueOf(null == itemRatioUserItemClickCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemClickCross.get(userId + "_" + skuId));
            features[206] = String.valueOf(null == itemRatioUserItemDetailCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemDetailCross.get(userId + "_" + skuId));
            features[207] = String.valueOf(null == itemRatioUserItemCartCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemCartCross.get(userId + "_" + skuId));
            features[208] = String.valueOf(null == itemRatioUserItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemCartDeleteCross.get(userId + "_" + skuId));
            features[209] = String.valueOf(null == itemRatioUserItemFollowCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemFollowCross.get(userId + "_" + skuId));

            features[210] = String.valueOf(null == userItemRatioUserClickCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserClickCross.get(userId + "_" + skuId));
            features[211] = String.valueOf(null == userItemRatioUserDetailCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserDetailCross.get(userId + "_" + skuId));
            features[212] = String.valueOf(null == userItemRatioUserCartCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserCartCross.get(userId + "_" + skuId));
            features[213] = String.valueOf(null == userItemRatioUserCartDeleteCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserCartDeleteCross.get(userId + "_" + skuId));
            features[214] = String.valueOf(null == userItemRatioUserFollowCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserFollowCross.get(userId + "_" + skuId));
            features[215] = String.valueOf(null == userItemRatioItemClickCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemClickCross.get(userId + "_" + skuId));
            features[216] = String.valueOf(null == userItemRatioItemDetailCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemDetailCross.get(userId + "_" + skuId));
            features[217] = String.valueOf(null == userItemRatioItemCartCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemCartCross.get(userId + "_" + skuId));
            features[218] = String.valueOf(null == userItemRatioItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemCartDeleteCross.get(userId + "_" + skuId));
            features[219] = String.valueOf(null == userItemRatioItemFollowCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemFollowCross.get(userId + "_" + skuId));
            features[220] = String.valueOf(null == userItemRatioUserItemClickCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemClickCross.get(userId + "_" + skuId));
            features[220] = String.valueOf(null == userItemRatioUserItemDetailCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemDetailCross.get(userId + "_" + skuId));
            features[222] = String.valueOf(null == userItemRatioUserItemCartCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemCartCross.get(userId + "_" + skuId));
            features[223] = String.valueOf(null == userItemRatioUserItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemCartDeleteCross.get(userId + "_" + skuId));
            features[224] = String.valueOf(null == userItemRatioUserItemFollowCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemFollowCross.get(userId + "_" + skuId));


            if(!isPredict){
                Long count = labelMap.get(userId + "_" + skuId);
                if(null == count){
                    features[225] = String.valueOf(1);
                    ++negative;
                }
                else {
                    features[225] = String.valueOf(0);
                    ++positive;
                }
            }

            lines.add(features);
        }

        log.info("positive: " + positive);
        log.info("negative: " + negative);

        CSVWriter writer = new CSVWriter(new FileWriter(path + filename), ',', '\0');
        if(isPredict){
            List<String> list = new ArrayList<>(attributes.keySet());
            List<String> header = list.subList(0, attributes.size() - 1);
            writer.writeNext(header.toArray(new String[0]));
        }
        else {
            writer.writeNext(attributes.keySet().toArray(new String[0]));
        }

        writer.writeAll(lines);
        writer.close();

        return lines;
    }

    private static void setFeature(String[] features, Map<String, Double> map, int index, boolean isRatio){
        if(isRatio){
            features[index] = String.valueOf(null == map.get("click") ? 0 : map.get("click"));
            features[index + 1] = String.valueOf(null == map.get("detail") ? 0 : map.get("detail"));
            features[index + 2] = String.valueOf(null == map.get("cart") ? 0 : map.get("cart"));
            features[index + 3] = String.valueOf(null == map.get("cartDelete") ? 0 : map.get("cartDelete"));
            features[index + 4] = String.valueOf(null == map.get("follow") ? 0 : map.get("follow"));
        }
        else {
            features[index] = String.valueOf(null == map.get("click") ? 0 : map.get("click"));
            features[index + 1] = String.valueOf(null == map.get("detail") ? 0 : map.get("detail"));
            features[index + 2] = String.valueOf(null == map.get("cart") ? 0 : map.get("cart"));
            features[index + 3] = String.valueOf(null == map.get("cartDelete") ? 0 : map.get("cartDelete"));
            features[index + 4] = String.valueOf(null == map.get("buy") ? 0 : map.get("buy"));
            features[index + 5] = String.valueOf(null == map.get("follow") ? 0 : map.get("follow"));
        }
    }
}

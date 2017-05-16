package com.jd.may.fifteen;

import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import au.com.bytecode.opencsv.CSVWriter;
import com.alibaba.fastjson.JSONObject;
import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-5-7.
 */
public class AllFeaturesForGBM {
    private static final Logger log = Logger.getLogger(AllFeaturesForGBM.class);
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
        attributes.put("user_follow_count", i++);
        attributes.put("user_click_sum", i++);
        attributes.put("user_detail_sum", i++);
        attributes.put("user_cart_sum", i++);
        attributes.put("user_cart_delete_sum", i++);
        attributes.put("user_follow_sum", i++);
        attributes.put("user_click_avg", i++);
        attributes.put("user_detail_avg", i++);
        attributes.put("user_cart_avg", i++);
        attributes.put("user_cart_delete_avg", i++);
        attributes.put("user_follow_avg", i++);

        attributes.put("item_click_count", i++);
        attributes.put("item_detail_count", i++);
        attributes.put("item_cart_count", i++);
        attributes.put("item_cart_delete_count", i++);
        attributes.put("item_follow_count", i++);
        attributes.put("item_click_sum", i++);
        attributes.put("item_detail_sum", i++);
        attributes.put("item_cart_sum", i++);
        attributes.put("item_cart_delete_sum", i++);
        attributes.put("item_follow_sum", i++);
        attributes.put("item_click_avg", i++);
        attributes.put("item_detail_avg", i++);
        attributes.put("item_cart_avg", i++);
        attributes.put("item_cart_delete_avg", i++);
        attributes.put("item_follow_avg", i++);

        attributes.put("user_item_click_count", i++);
        attributes.put("user_item_detail_count", i++);
        attributes.put("user_item_cart_count", i++);
        attributes.put("user_item_cart_delete_count", i++);
        attributes.put("user_item_follow_count", i++);
        attributes.put("user_item_click_sum", i++);
        attributes.put("user_item_detail_sum", i++);
        attributes.put("user_item_cart_sum", i++);
        attributes.put("user_item_cart_delete_sum", i++);
        attributes.put("user_item_follow_sum", i++);
        attributes.put("user_item_click_avg", i++);
        attributes.put("user_item_detail_avg", i++);
        attributes.put("user_item_cart_avg", i++);
        attributes.put("user_item_cart_delete_avg", i++);
        attributes.put("user_item_follow_avg", i++);

        attributes.put("user_click_count_rank", i++);
        attributes.put("user_detail_count_rank", i++);
        attributes.put("user_cart_count_rank", i++);
        attributes.put("user_cart_delete_count_rank", i++);
        attributes.put("user_follow_count_rank", i++);
        attributes.put("user_click_sum_rank", i++);
        attributes.put("user_detail_sum_rank", i++);
        attributes.put("user_cart_sum_rank", i++);
        attributes.put("user_cart_delete_sum_rank", i++);
        attributes.put("user_follow_sum_rank", i++);
        attributes.put("user_click_avg_rank", i++);
        attributes.put("user_detail_avg_rank", i++);
        attributes.put("user_cart_avg_rank", i++);
        attributes.put("user_cart_delete_avg_rank", i++);
        attributes.put("user_follow_avg_rank", i++);

        attributes.put("item_click_count_rank", i++);
        attributes.put("item_detail_count_rank", i++);
        attributes.put("item_cart_count_rank", i++);
        attributes.put("item_cart_delete_count_rank", i++);
        attributes.put("item_follow_count_rank", i++);
        attributes.put("item_click_sum_rank", i++);
        attributes.put("item_detail_sum_rank", i++);
        attributes.put("item_cart_sum_rank", i++);
        attributes.put("item_cart_delete_sum_rank", i++);
        attributes.put("item_follow_sum_rank", i++);
        attributes.put("item_click_avg_rank", i++);
        attributes.put("item_detail_avg_rank", i++);
        attributes.put("item_cart_avg_rank", i++);
        attributes.put("item_cart_delete_avg_rank", i++);
        attributes.put("item_follow_avg_rank", i++);

        attributes.put("user_item_click_count_rank", i++);
        attributes.put("user_item_detail_count_rank", i++);
        attributes.put("user_item_cart_count_rank", i++);
        attributes.put("user_item_cart_delete_count_rank", i++);
        attributes.put("user_item_follow_count_rank", i++);
        attributes.put("user_item_click_sum_rank", i++);
        attributes.put("user_item_detail_sum_rank", i++);
        attributes.put("user_item_cart_sum_rank", i++);
        attributes.put("user_item_cart_delete_sum_rank", i++);
        attributes.put("user_item_follow_sum_rank", i++);
        attributes.put("user_item_click_avg_rank", i++);
        attributes.put("user_item_detail_avg_rank", i++);
        attributes.put("user_item_cart_avg_rank", i++);
        attributes.put("user_item_cart_delete_avg_rank", i++);
        attributes.put("user_item_follow_avg_rank", i++);

        attributes.put("user_popular", i++);
        attributes.put("user_popular_rank", i++);
        attributes.put("item_popular", i++);
        attributes.put("item_popular_rank", i++);

        attributes.put("item_action_user", i++);
        attributes.put("item_action_user_rank", i++);

        attributes.put("user_distinct_item_click", i++);
        attributes.put("user_distinct_item_detail", i++);
        attributes.put("user_distinct_item_cart", i++);
        attributes.put("user_distinct_item_cart_delete", i++);
        attributes.put("user_distinct_item_follow", i++);
        attributes.put("user_distinct_item_click_rank", i++);
        attributes.put("user_distinct_item_detail_rank", i++);
        attributes.put("user_distinct_item_cart_rank", i++);
        attributes.put("user_distinct_item_cart_delete_rank", i++);
        attributes.put("user_distinct_item_follow_rank", i++);

        attributes.put("item_distinct_user_click", i++);
        attributes.put("item_distinct_user_detail", i++);
        attributes.put("item_distinct_user_cart", i++);
        attributes.put("item_distinct_user_cart_delete", i++);
        attributes.put("item_distinct_user_follow", i++);
        attributes.put("item_distinct_user_click_rank", i++);
        attributes.put("item_distinct_user_detail_rank", i++);
        attributes.put("item_distinct_user_cart_rank", i++);
        attributes.put("item_distinct_user_cart_delete_rank", i++);
        attributes.put("item_distinct_user_follow_rank", i++);

        attributes.put("target", i++);
        log.info("feature size: " + attributes.size());
    }

    public static List<String[]> features(String filename, String tablename, boolean isPredict) throws Exception{
        List<String[]> lines = new ArrayList<>();

        /*Map<String,Map<String, Object>> userInfos = new HashMap<>();
        String userInfoSql = "select user_id,sku_id,max(age) as age,max(sex) as sex,max(user_level) as user_level,max(attr1) as attr1,max(attr2) as attr2,max(attr3) as attr3,max(cate) as cate,max(brand) as brand,max(comment_num) as comment_num,max(has_bad_comment) as has_bad_comment,max(bad_comment_rate) as bad_comment_rate from " + tablename + " group by user_id,sku_id";
        log.info("user info sql: " + userInfoSql);
        List<Map<String, Object>> result = DBOperation.queryBySql(userInfoSql);
        log.info("user info size: " + result.size());
        for(Map<String, Object> row : result){
            String userId = String.valueOf(row.get("user_id"));
            String skuId = String.valueOf(row.get("sku_id"));
            userInfos.put(userId + "_" + skuId, row);
        }

        String userCountSql = "select user_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(follow>0,1,null))+1),3) as follow from " + tablename + " group by user_id";
        String userSumSql = "select user_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(follow)+1),3) as follow from " + tablename + " group by user_id";
        String userAvgSql = "select user_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(follow)+1),3) as follow from " + tablename + " group by user_id";
        String itemCountSql = "select sku_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(follow>0,1,null))+1),3) as follow from " + tablename + " group by sku_id";
        String itemSumSql = "select sku_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(follow)+1),3) as follow from " + tablename + " group by sku_id";
        String itemAvgSql = "select sku_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(follow)+1),3) as follow from " + tablename + " group by sku_id";
        String userItemCountSql = "select user_id,sku_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(follow>0,1,null))+1),3) as follow from " + tablename + " group by user_id,sku_id";
        String userItemSumSql = "select user_id,sku_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(follow)+1),3) as follow from " + tablename + " group by user_id,sku_id";
        String userItemAvgSql = "select user_id,sku_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(follow)+1),3) as follow from " + tablename + " group by user_id,sku_id";

        Map<String, Map<String, Double>> userCountFeature = Features.countFeature(userCountSql, 0, 0);
        Map<String, Map<String, Double>> userSumFeature = Features.sumFeature(userSumSql, 0);
        Map<String, Map<String, Double>> userAvgFeature = Features.avgFeature(userAvgSql, 0);
        Map<String, Map<String, Double>> itemCountFeature = Features.countFeature(itemCountSql, 1, 0);
        Map<String, Map<String, Double>> itemSumFeature = Features.sumFeature(itemSumSql, 1);
        Map<String, Map<String, Double>> itemAvgFeature = Features.avgFeature(itemAvgSql, 1);
        Map<String, Map<String, Double>> userItemCountFeature = Features.countFeature(userItemCountSql, 2, 0);
        Map<String, Map<String, Double>> userItemSumFeature = Features.sumFeature(userItemSumSql, 2);
        Map<String, Map<String, Double>> userItemAvgFeature = Features.avgFeature(userItemAvgSql, 2);

        Map<String, Integer> userClickCountRank = Rank.rank(userCountFeature, "click");
        Map<String, Integer> userDetailCountRank = Rank.rank(userCountFeature, "detail");
        Map<String, Integer> userCartCountRank = Rank.rank(userCountFeature, "cart");
        Map<String, Integer> userCartDeleteCountRank = Rank.rank(userCountFeature, "cartDelete");
        Map<String, Integer> userFollowCountRank = Rank.rank(userCountFeature, "follow");
        Map<String, Integer> userClickSumRank = Rank.rank(userSumFeature, "click");
        Map<String, Integer> userDetailSumRank = Rank.rank(userSumFeature, "detail");
        Map<String, Integer> userCartSumRank = Rank.rank(userSumFeature, "cart");
        Map<String, Integer> userCartDeleteSumRank = Rank.rank(userSumFeature, "cartDelete");
        Map<String, Integer> userFollowSumRank = Rank.rank(userSumFeature, "follow");
        Map<String, Integer> userClickAvgRank = Rank.rank(userAvgFeature, "click");
        Map<String, Integer> userDetailAvgRank = Rank.rank(userAvgFeature, "detail");
        Map<String, Integer> userCartAvgRank = Rank.rank(userAvgFeature, "cart");
        Map<String, Integer> userCartDeleteAvgRank = Rank.rank(userAvgFeature, "cartDelete");
        Map<String, Integer> userFollowAvgRank = Rank.rank(userAvgFeature, "follow");
        Map<String, Integer> itemClickCountRank = Rank.rank(itemCountFeature, "click");
        Map<String, Integer> itemDetailCountRank = Rank.rank(itemCountFeature, "detail");
        Map<String, Integer> itemCartCountRank = Rank.rank(itemCountFeature, "cart");
        Map<String, Integer> itemCartDeleteCountRank = Rank.rank(itemCountFeature, "cartDelete");
        Map<String, Integer> itemFollowCountRank = Rank.rank(itemCountFeature, "follow");
        Map<String, Integer> itemClickSumRank = Rank.rank(itemSumFeature, "click");
        Map<String, Integer> itemDetailSumRank = Rank.rank(itemSumFeature, "detail");
        Map<String, Integer> itemCartSumRank = Rank.rank(itemSumFeature, "cart");
        Map<String, Integer> itemCartDeleteSumRank = Rank.rank(itemSumFeature, "cartDelete");
        Map<String, Integer> itemFollowSumRank = Rank.rank(itemSumFeature, "follow");
        Map<String, Integer> itemClickAvgRank = Rank.rank(itemAvgFeature, "click");
        Map<String, Integer> itemDetailAvgRank = Rank.rank(itemAvgFeature, "detail");
        Map<String, Integer> itemCartAvgRank = Rank.rank(itemAvgFeature, "cart");
        Map<String, Integer> itemCartDeleteAvgRank = Rank.rank(itemAvgFeature, "cartDelete");
        Map<String, Integer> itemFollowAvgRank = Rank.rank(itemAvgFeature, "follow");
        Map<String, Integer> userItemClickCountRank = Rank.rank(userItemCountFeature, "click");
        Map<String, Integer> userItemDetailCountRank = Rank.rank(userItemCountFeature, "detail");
        Map<String, Integer> userItemCartCountRank = Rank.rank(userItemCountFeature, "cart");
        Map<String, Integer> userItemCartDeleteCountRank = Rank.rank(userItemCountFeature, "cartDelete");
        Map<String, Integer> userItemFollowCountRank = Rank.rank(userItemCountFeature, "follow");
        Map<String, Integer> userItemClickSumRank = Rank.rank(userItemSumFeature, "click");
        Map<String, Integer> userItemDetailSumRank = Rank.rank(userItemSumFeature, "detail");
        Map<String, Integer> userItemCartSumRank = Rank.rank(userItemSumFeature, "cart");
        Map<String, Integer> userItemCartDeleteSumRank = Rank.rank(userItemSumFeature, "cartDelete");
        Map<String, Integer> userItemFollowSumRank = Rank.rank(userItemSumFeature, "follow");
        Map<String, Integer> userItemClickAvgRank = Rank.rank(userItemAvgFeature, "click");
        Map<String, Integer> userItemDetailAvgRank = Rank.rank(userItemAvgFeature, "detail");
        Map<String, Integer> userItemCartAvgRank = Rank.rank(userItemAvgFeature, "cart");
        Map<String, Integer> userItemCartDeleteAvgRank = Rank.rank(userItemAvgFeature, "cartDelete");
        Map<String, Integer> userItemFollowAvgRank = Rank.rank(userItemAvgFeature, "follow");

        Map<String, Double> userPopular = Popular.userPopular(tablename);
        Map<String, Integer> userPopularRank = Rank.rank(userPopular);
        Map<String, Double> itemPopular = Popular.itemPopular(tablename);
        Map<String, Integer> itemPopularRank = Rank.rank(itemPopular);


        Map<String, Double> itemActionUserCount = Popular.itemActionUserCount(tablename);
        Map<String, Integer> itemActionUserRank = Rank.rank(itemActionUserCount);

        String userItemClickSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where click>0 group by user_id";
        Map<String, Double> userItemClick = ItemBuyUsers.itemUserCount(userItemClickSql, "user_id");
        String userItemDetailSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where detail>0 group by user_id";
        Map<String, Double> userItemDetail = ItemBuyUsers.itemUserCount(userItemDetailSql, "user_id");
        String userItemCartSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where cart>0 group by user_id";
        Map<String, Double> userItemCart = ItemBuyUsers.itemUserCount(userItemCartSql, "user_id");
        String userItemCartDeleteSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where cart_delete>0 group by user_id";
        Map<String, Double> userItemCartDelete = ItemBuyUsers.itemUserCount(userItemCartDeleteSql, "user_id");
        String userItemFollowSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where follow>0 group by user_id";
        Map<String, Double> userItemFollow = ItemBuyUsers.itemUserCount(userItemFollowSql, "user_id");
        Map<String, Integer> userItemClickRank = Rank.rank(userItemClick);
        Map<String, Integer> userItemDetailRank = Rank.rank(userItemDetail);
        Map<String, Integer> userItemCartRank = Rank.rank(userItemCart);
        Map<String, Integer> userItemCartDeleteRank = Rank.rank(userItemCartDelete);
        Map<String, Integer> userItemFollowRank = Rank.rank(userItemFollow);

        String itemUserClickSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where click >0 group by sku_id";
        Map<String, Double> itemUserClick = ItemBuyUsers.itemUserCount(itemUserClickSql, "sku_id");
        String itemUserDetailSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where detail>0 group by sku_id";
        Map<String, Double> itemUserDetail = ItemBuyUsers.itemUserCount(itemUserDetailSql, "sku_id");
        String itemUserCartSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where cart>0 group by sku_id";
        Map<String, Double> itemUserCart = ItemBuyUsers.itemUserCount(itemUserCartSql, "sku_id");
        String itemUserCartDeleteSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where cart_delete>0 group by sku_id";
        Map<String, Double> itemUserCartDelete = ItemBuyUsers.itemUserCount(itemUserCartDeleteSql, "sku_id");
        String itemUserFollowSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where follow>0 group by sku_id";
        Map<String, Double> itemUserFollow = ItemBuyUsers.itemUserCount(itemUserFollowSql, "sku_id");
        Map<String, Integer> itemUserClickRank = Rank.rank(itemUserClick);
        Map<String, Integer> itemUserDetailRank = Rank.rank(itemUserDetail);
        Map<String, Integer> itemUserCartRank = Rank.rank(itemUserCart);
        Map<String, Integer> itemUserCartDeleteRank = Rank.rank(itemUserCartDelete);
        Map<String, Integer> itemUserFollowRank = Rank.rank(itemUserFollow);

        Map<String, Long> labelMap = new HashMap<>();
        if(!isPredict){
            String labelSql = "select user_id,sku_id,count(1) as count from " + tablename + " where buy>0 group by user_id,sku_id";
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
        String sql = "select user_id,sku_id,group_concat(model_id separator '_') as model_id from " + tablename + " group by user_id,sku_id";
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

                for(Entry<String, Integer> entry : modelIdMap.entrySet()){
                    int index = attributes.get("model_" + entry.getKey());
                    features[index] = String.valueOf(entry.getValue());
                }
            }

            setFeature(features, userCountFeature.get(userId), 103);
            setFeature(features, userSumFeature.get(userId), 108);
            setFeature(features, userAvgFeature.get(userId), 113);

            setFeature(features, itemCountFeature.get(skuId), 118);
            setFeature(features, itemSumFeature.get(skuId), 123);
            setFeature(features, itemAvgFeature.get(skuId), 128);

            setFeature(features, userItemCountFeature.get(userId + "_" + skuId), 133);
            setFeature(features, userItemSumFeature.get(userId + "_" + skuId), 138);
            setFeature(features, userItemAvgFeature.get(userId + "_" + skuId), 143);

            features[148] = String.valueOf(null == userClickCountRank.get(userId) ? 0 : userClickCountRank.get(userId));
            features[149] = String.valueOf(null == userDetailCountRank.get(userId) ? 0 : userDetailCountRank.get(userId));
            features[150] = String.valueOf(null == userCartCountRank.get(userId) ? 0 : userCartCountRank.get(userId));
            features[151] = String.valueOf(null == userCartDeleteCountRank.get(userId) ? 0 : userCartDeleteCountRank.get(userId));
            features[152] = String.valueOf(null == userFollowCountRank.get(userId) ? 0 : userFollowCountRank.get(userId));
            features[153] = String.valueOf(null == userClickSumRank.get(userId) ? 0 : userClickSumRank.get(userId));
            features[154] = String.valueOf(null == userDetailSumRank.get(userId) ? 0 : userDetailSumRank.get(userId));
            features[155] = String.valueOf(null == userCartSumRank.get(userId) ? 0 : userCartSumRank.get(userId));
            features[156] = String.valueOf(null == userCartDeleteSumRank.get(userId) ? 0 : userCartDeleteSumRank.get(userId));
            features[157] = String.valueOf(null == userFollowSumRank.get(userId) ? 0 : userFollowSumRank.get(userId));
            features[158] = String.valueOf(null == userClickAvgRank.get(userId) ? 0 : userClickAvgRank.get(userId));
            features[159] = String.valueOf(null == userDetailAvgRank.get(userId) ? 0 : userDetailAvgRank.get(userId));
            features[160] = String.valueOf(null == userCartAvgRank.get(userId) ? 0 : userCartAvgRank.get(userId));
            features[161] = String.valueOf(null == userCartDeleteAvgRank.get(userId) ? 0 : userCartDeleteAvgRank.get(userId));
            features[162] = String.valueOf(null == userFollowAvgRank.get(userId) ? 0 : userFollowAvgRank.get(userId));
            features[163] = String.valueOf(null == itemClickCountRank.get(skuId) ? 0 : itemClickCountRank.get(skuId));
            features[164] = String.valueOf(null == itemDetailCountRank.get(skuId) ? 0 : itemDetailCountRank.get(skuId));
            features[165] = String.valueOf(null == itemCartCountRank.get(skuId) ? 0 : itemCartCountRank.get(skuId));
            features[166] = String.valueOf(null == itemCartDeleteCountRank.get(skuId) ? 0 : itemCartDeleteCountRank.get(skuId));
            features[167] = String.valueOf(null == itemFollowCountRank.get(skuId) ? 0 : itemFollowCountRank.get(skuId));
            features[168] = String.valueOf(null == itemClickSumRank.get(skuId) ? 0 : itemClickSumRank.get(skuId));
            features[169] = String.valueOf(null == itemDetailSumRank.get(skuId) ? 0 : itemDetailSumRank.get(skuId));
            features[170] = String.valueOf(null == itemCartSumRank.get(skuId) ? 0 : itemCartSumRank.get(skuId));
            features[171] = String.valueOf(null == itemCartDeleteSumRank.get(skuId) ? 0 : itemCartDeleteSumRank.get(skuId));
            features[172] = String.valueOf(null == itemFollowSumRank.get(skuId) ? 0 : itemFollowSumRank.get(skuId));
            features[173] = String.valueOf(null == itemClickAvgRank.get(skuId) ? 0 : itemClickAvgRank.get(skuId));
            features[174] = String.valueOf(null == itemDetailAvgRank.get(skuId) ? 0 : itemDetailAvgRank.get(skuId));
            features[175] = String.valueOf(null == itemCartAvgRank.get(skuId) ? 0 : itemCartAvgRank.get(skuId));
            features[176] = String.valueOf(null == itemCartDeleteAvgRank.get(skuId) ? 0 : itemCartDeleteAvgRank.get(skuId));
            features[177] = String.valueOf(null == itemFollowAvgRank.get(skuId) ? 0 : itemFollowAvgRank.get(skuId));
            features[178] = String.valueOf(null == userItemClickCountRank.get(userId + "_" + skuId) ? 0 : userItemClickCountRank.get(userId + "_" + skuId));
            features[179] = String.valueOf(null == userItemDetailCountRank.get(userId + "_" + skuId) ? 0 : userItemDetailCountRank.get(userId + "_" + skuId));
            features[180] = String.valueOf(null == userItemCartCountRank.get(userId + "_" + skuId) ? 0 : userItemCartCountRank.get(userId + "_" + skuId));
            features[181] = String.valueOf(null == userItemCartDeleteCountRank.get(userId + "_" + skuId) ? 0 : userItemCartDeleteCountRank.get(userId + "_" + skuId));
            features[182] = String.valueOf(null == userItemFollowCountRank.get(userId + "_" + skuId) ? 0 : userItemFollowCountRank.get(userId + "_" + skuId));
            features[183] = String.valueOf(null == userItemClickSumRank.get(userId + "_" + skuId) ? 0 : userItemClickSumRank.get(userId + "_" + skuId));
            features[184] = String.valueOf(null == userItemDetailSumRank.get(userId + "_" + skuId) ? 0 : userItemDetailSumRank.get(userId + "_" + skuId));
            features[185] = String.valueOf(null == userItemCartSumRank.get(userId + "_" + skuId) ? 0 : userItemCartSumRank.get(userId + "_" + skuId));
            features[186] = String.valueOf(null == userItemCartDeleteSumRank.get(userId + "_" + skuId) ? 0 : userItemCartDeleteSumRank.get(userId + "_" + skuId));
            features[187] = String.valueOf(null == userItemFollowSumRank.get(userId + "_" + skuId) ? 0 : userItemFollowSumRank.get(userId + "_" + skuId));
            features[188] = String.valueOf(null == userItemClickAvgRank.get(userId + "_" + skuId) ? 0 : userItemClickAvgRank.get(userId + "_" + skuId));
            features[189] = String.valueOf(null == userItemDetailAvgRank.get(userId + "_" + skuId) ? 0 : userItemDetailAvgRank.get(userId + "_" + skuId));
            features[190] = String.valueOf(null == userItemCartAvgRank.get(userId + "_" + skuId) ? 0 : userItemCartAvgRank.get(userId + "_" + skuId));
            features[191] = String.valueOf(null == userItemCartDeleteAvgRank.get(userId + "_" + skuId) ? 0 : userItemCartDeleteAvgRank.get(userId + "_" + skuId));
            features[192] = String.valueOf(null == userItemFollowAvgRank.get(userId + "_" + skuId) ? 0 : userItemFollowAvgRank.get(userId + "_" + skuId));

            features[193] = String.valueOf(null == userPopular.get(userId) ? 0 : userPopular.get(userId));
            features[194] = String.valueOf(null == userPopularRank.get(userId) ? 0 : userPopularRank.get(userId));
            features[195] = String.valueOf(null == itemPopular.get(skuId) ? 0 : itemPopular.get(skuId));
            features[196] = String.valueOf(null == itemPopularRank.get(skuId) ? 0 : itemPopularRank.get(skuId));
            features[197] = String.valueOf(null == itemActionUserCount.get(skuId) ? 0 : itemActionUserCount.get(skuId));
            features[198] = String.valueOf(null == itemActionUserRank.get(skuId) ? 0 : itemActionUserRank.get(skuId));

            features[199] = String.valueOf(null == userItemClick.get(userId) ? 0 : userItemClick.get(userId));
            features[200] = String.valueOf(null == userItemDetail.get(userId) ? 0 : userItemDetail.get(userId));
            features[201] = String.valueOf(null == userItemCart.get(userId) ? 0 : userItemCart.get(userId));
            features[202] = String.valueOf(null == userItemCartDelete.get(userId) ? 0 : userItemCartDelete.get(userId));
            features[203] = String.valueOf(null == userItemFollow.get(userId) ? 0 : userItemFollow.get(userId));

            features[204] = String.valueOf(null == userItemClickRank.get(userId) ? 0 : userItemClickRank.get(userId));
            features[205] = String.valueOf(null == userItemDetailRank.get(userId) ? 0 : userItemDetailRank.get(userId));
            features[206] = String.valueOf(null == userItemCartRank.get(userId) ? 0 : userItemCartRank.get(userId));
            features[207] = String.valueOf(null == userItemCartDeleteRank.get(userId) ? 0 : userItemCartDeleteRank.get(userId));
            features[208] = String.valueOf(null == userItemFollowRank.get(userId) ? 0 : userItemFollowRank.get(userId));

            features[209] = String.valueOf(null == itemUserClick.get(skuId) ? 0 : itemUserClick.get(skuId));
            features[210] = String.valueOf(null == itemUserDetail.get(skuId) ? 0 : itemUserDetail.get(skuId));
            features[211] = String.valueOf(null == itemUserCart.get(skuId) ? 0 : itemUserCart.get(skuId));
            features[212] = String.valueOf(null == itemUserCartDelete.get(skuId) ? 0 : itemUserCartDelete.get(skuId));
            features[213] = String.valueOf(null == itemUserFollow.get(skuId) ? 0 : itemUserFollow.get(skuId));

            features[214] = String.valueOf(null == itemUserClickRank.get(skuId) ? 0 : itemUserClickRank.get(skuId));
            features[215] = String.valueOf(null == itemUserDetailRank.get(skuId) ? 0 : itemUserDetailRank.get(skuId));
            features[216] = String.valueOf(null == itemUserCartRank.get(skuId) ? 0 : itemUserCartRank.get(skuId));
            features[217] = String.valueOf(null == itemUserCartDeleteRank.get(skuId) ? 0 : itemUserCartDeleteRank.get(skuId));
            features[218] = String.valueOf(null == itemUserFollowRank.get(skuId) ? 0 : itemUserFollowRank.get(skuId));

            if(!isPredict){
                Long count = labelMap.get(userId + "_" + skuId);
                if(null == count){
                    features[219] = String.valueOf(1);
                    ++negative;
                }
                else {
                    features[219] = String.valueOf(0);
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
        writer.close();*/

        return lines;
    }

    private static void setFeature(String[] features, Map<String, Double> map, int index){
        features[index] = String.valueOf(null == map.get("click") ? 0 : map.get("click"));
        features[index + 1] = String.valueOf(null == map.get("detail") ? 0 : map.get("detail"));
        features[index + 2] = String.valueOf(null == map.get("cart") ? 0 : map.get("cart"));
        features[index + 3] = String.valueOf(null == map.get("cartDelete") ? 0 : map.get("cartDelete"));
        features[index + 4] = String.valueOf(null == map.get("follow") ? 0 : map.get("follow"));
    }
}

























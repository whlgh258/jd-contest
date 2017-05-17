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
import com.alibaba.fastjson.parser.Feature;
import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-5-14.
 */
public class AllFeaturesForDL {
    private static final Logger log = Logger.getLogger(AllFeaturesForDL.class);

    private static final int[] modelIds = new int[]{11,217,27,216,17,210,14,211,218,26,28,24,220,21,25,15,221,222,29,23,19,16,223,219,116,224,22,31,125,13,111,18,33,119,333,112,322,311,318,110,319,34,12,124,120,121,325,331,329,334,346,328,32,320,341,348,340,323,337,343,345,336,312,321,335,347,344,342,316,315,36,115,114,113,122,317,212,313,35,314,39,338,225,310,324,332,327,37,38,330};
    private static final Map<Integer, Double> decayMap;
    private static final Map<String, Map<String, Object>> userInfos;
    private static final Map<String, Map<String, Object>> itemInfos;
    private static final Map<String, Map<String, Object>> commentInfos;

    private static final Map<String, Object> cache = new HashMap<>();

    static {
        userInfos = new HashMap<>();
        String userInfoSql = "select user_id,max(age) as age,max(sex) as sex,max(user_level) as user_level from user_action_1 group by user_id";
        log.info("user info sql: " + userInfoSql);
        List<Map<String, Object>> result = DBOperation.queryBySql(userInfoSql);
        log.info("user info size: " + result.size());
        for(Map<String, Object> row : result){
            String userId = String.valueOf(row.get("user_id"));
            userInfos.put(userId, row);
        }

        itemInfos = new HashMap<>();
        String itemInfoSql = "select sku_id,max(attr1) as attr1,max(attr2) as attr2,max(attr3) as attr3,max(cate) as cate,max(brand) as brand from product group by sku_id";
        log.info("item info sql: " + itemInfoSql);
        List<Map<String, Object>> itemResult = DBOperation.queryBySql(itemInfoSql);
        log.info("item info size: " + itemResult.size());
        for(Map<String, Object> row : itemResult){
            String skuId = String.valueOf(row.get("sku_id"));
            itemInfos.put(skuId, row);
        }

        commentInfos = new HashMap<>();
        String commentInfoSql = "select sku_id,max(comment_num) as comment_num,max(has_bad_comment) as has_bad_comment,max(bad_comment_rate) as bad_comment_rate from comment group by sku_id";
        log.info("comment info sql: " + commentInfoSql);
        List<Map<String, Object>> commentResult = DBOperation.queryBySql(commentInfoSql);
        log.info("comment info size: " + commentResult.size());
        for(Map<String, Object> row : commentResult){
            String skuId = String.valueOf(row.get("sku_id"));
            commentInfos.put(skuId, row);
        }

        decayMap = new HashMap<>();
        decayMap.put(-1, 1.0);
        decayMap.put(-2, 0.5);
        decayMap.put(-3, 0.333);
        decayMap.put(-4, 0.25);
        decayMap.put(-5, 0.2);
        decayMap.put(-6, 0.167);
        decayMap.put(-7, 0.143);
        decayMap.put(-8, 0.125);
        decayMap.put(-9, 0.111);
        decayMap.put(-10, 0.1);
        decayMap.put(-11, 0.091);
        decayMap.put(-12, 0.083);
        decayMap.put(-13, 0.077);
        decayMap.put(-14, 0.071);
        decayMap.put(-15, 0.067);
    }

    public static void features(String filename, int labelPeriod, int trainStartPeriod, int trainEndPeriod, String tablename, boolean isPredict, String path) throws Exception{
        log.info("train start period: " + trainStartPeriod);
        log.info("train end period: " + trainEndPeriod);
        log.info("label period: " + labelPeriod);

        List<Map<String, Object>> lines = new ArrayList<>();
        Map<String, Map<String, Double>> userCountFeature = new HashMap<>();
        Map<String, Map<String, Double>> userSumFeature = new HashMap<>();
        Map<String, Map<String, Double>> userAvgFeature = new HashMap<>();
        Map<String, Map<String, Double>> itemCountFeature = new HashMap<>();
        Map<String, Map<String, Double>> itemSumFeature = new HashMap<>();
        Map<String, Map<String, Double>> itemAvgFeature = new HashMap<>();
        Map<String, Map<String, Double>> userItemCountFeature = new HashMap<>();
        Map<String, Map<String, Double>> userItemSumFeature = new HashMap<>();
        Map<String, Map<String, Double>> userItemAvgFeature = new HashMap<>();

        Map<String, Map<String, Double>> userPopularMap = new HashMap<>();
        Map<String, Map<String, Double>> itemPopularMap = new HashMap<>();
        Map<String, Map<String, Double>> itemActionUserCountMap = new HashMap<>();

        Map<String, Map<String, Double>> userItemClickMap = new HashMap<>();
        Map<String, Map<String, Double>> userItemDetailMap = new HashMap<>();
        Map<String, Map<String, Double>> userItemCartMap = new HashMap<>();
        Map<String, Map<String, Double>> userItemCartDeleteMap = new HashMap<>();
        Map<String, Map<String, Double>> userItemFollowMap = new HashMap<>();
        Map<String, Map<String, Double>> itemUserClickMap = new HashMap<>();
        Map<String, Map<String, Double>> itemUserDetailMap = new HashMap<>();
        Map<String, Map<String, Double>> itemUserCartMap = new HashMap<>();
        Map<String, Map<String, Double>> itemUserCartDeleteMap = new HashMap<>();
        Map<String, Map<String, Double>> itemUserFollowMap = new HashMap<>();

        for(int i = trainStartPeriod; i <= trainEndPeriod; i++) {
            int diff = labelPeriod - i;
            double decay = decayMap.get(diff);

            Map<String, Map<String, Double>> feature = null;
            String userCountSql = "select user_id,log(count(if(click>0,1,null))+1) as click,log(count(if(detail>0,1,null))+1) as detail,log(count(if(cart>0,1,null))+1) as cart,log(count(if(cart_delete>0,1,null))+1) as cart_delete,log(count(if(follow>0,1,null))+1) as follow from " + tablename + " where action_period=" + i + " group by user_id";
            if(null == cache.get(userCountSql)){
                feature = Features.feature(userCountSql, 0, "user_count_", decay, labelPeriod, i);
                cache.put(userCountSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(userCountSql);
            }
            log.info("trainPeriod: " + i + ", user count size: " + feature.size());
            mergeMap(userCountFeature, feature);

            String userSumSql = "select user_id,log(sum(click)+1) as click,log(sum(detail>0)+1) as detail,log(sum(cart)+1) as cart,log(sum(cart_delete)+1) as cart_delete,log(sum(follow)+1) as follow from " + tablename + " where action_period=" + i + " group by user_id";
            if(null == cache.get(userSumSql)){
                feature = Features.feature(userSumSql, 0, "user_sum_", decay, labelPeriod, i);
                cache.put(userSumSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(userSumSql);
            }
            log.info("trainPeriod: " + i + ", user sum size: " + feature.size());
            mergeMap(userSumFeature, feature);

            String userAvgSql = "select user_id,log(avg(click)+1) as click,log(avg(detail>0)+1) as detail,log(avg(cart)+1) as cart,log(avg(cart_delete)+1) as cart_delete,log(avg(follow)+1) as follow from " + tablename + " where action_period=" + i + " group by user_id";
            if(null == cache.get(userAvgSql)){
                feature = Features.feature(userAvgSql, 0, "user_avg_", decay, labelPeriod, i);
                cache.put(userAvgSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(userAvgSql);
            }
            log.info("trainPeriod: " + i + ", user avg size: " + feature.size());
            mergeMap(userAvgFeature, feature);

            String itemCountSql = "select sku_id,log(count(if(click>0,1,null))+1) as click,log(count(if(detail>0,1,null))+1) as detail,log(count(if(cart>0,1,null))+1) as cart,log(count(if(cart_delete>0,1,null))+1) as cart_delete,log(count(if(follow>0,1,null))+1) as follow from " + tablename + " where action_period=" + i + " group by sku_id";
            if(null == cache.get(itemCountSql)){
                feature = Features.feature(itemCountSql, 1, "item_count_", decay, labelPeriod, i);
                cache.put(itemCountSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(itemCountSql);
            }
            log.info("trainPeriod: " + i + ", item count size: " + feature.size());
            mergeMap(itemCountFeature, feature);

            String itemSumSql = "select sku_id,log(sum(click)+1) as click,log(sum(detail>0)+1) as detail,log(sum(cart)+1) as cart,log(sum(cart_delete)+1) as cart_delete,log(sum(follow)+1) as follow from " + tablename + " where action_period=" + i + " group by sku_id";
            if(null == cache.get(itemSumSql)){
                feature = Features.feature(itemSumSql, 1, "item_sum_", decay, labelPeriod, i);
                cache.put(itemSumSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(itemSumSql);
            }
            log.info("trainPeriod: " + i + ", item sum size: " + feature.size());
            mergeMap(itemSumFeature, feature);

            String itemAvgSql = "select sku_id,log(avg(click)+1) as click,log(avg(detail>0)+1) as detail,log(avg(cart)+1) as cart,log(avg(cart_delete)+1) as cart_delete,log(avg(follow)+1) as follow from " + tablename + " where action_period=" + i + " group by sku_id";
            if(null == cache.get(itemAvgSql)){
                feature = Features.feature(itemAvgSql, 1, "item_avg_", decay, labelPeriod, i);
                cache.put(itemAvgSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(itemAvgSql);
            }
            log.info("trainPeriod: " + i + ", item avg size: " + feature.size());
            mergeMap(itemAvgFeature, feature);

            String userItemCountSql = "select user_id,sku_id,log(count(if(click>0,1,null))+1) as click,log(count(if(detail>0,1,null))+1) as detail,log(count(if(cart>0,1,null))+1) as cart,log(count(if(cart_delete>0,1,null))+1) as cart_delete,log(count(if(follow>0,1,null))+1) as follow from " + tablename + " where action_period=" + i + " group by user_id,sku_id";
            if(null == cache.get(userItemCountSql)){
                feature = Features.feature(userItemCountSql, 2, "user_item_count_", decay, labelPeriod, i);
                cache.put(userItemCountSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(userItemCountSql);
            }
            log.info("trainPeriod: " + i + ", user item count size: " + feature.size());
            mergeMap(userItemCountFeature, feature);

            String userItemSumSql = "select user_id,sku_id,log(sum(click)+1) as click,log(sum(detail>0)+1) as detail,log(sum(cart)+1) as cart,log(sum(cart_delete)+1) as cart_delete,log(sum(follow)+1) as follow from " + tablename + " where action_period=" + i + " group by user_id,sku_id";
            if(null == cache.get(userItemSumSql)){
                feature = Features.feature(userItemSumSql, 2, "user_item_sum_", decay, labelPeriod, i);
                cache.put(userItemSumSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(userItemSumSql);
            }
            log.info("trainPeriod: " + i + ", user item sum size: " + feature.size());
            mergeMap(userItemSumFeature, feature);

            String userItemAvgSql = "select user_id,sku_id,log(avg(click)+1) as click,log(avg(detail>0)+1) as detail,log(avg(cart)+1) as cart,log(avg(cart_delete)+1) as cart_delete,log(avg(follow)+1) as follow from " + tablename + " where action_period=" + i + " group by user_id,sku_id";
            if(null == cache.get(userItemAvgSql)){
                feature = Features.feature(userItemAvgSql, 2, "user_item_avg_", decay, labelPeriod, i);
                cache.put(userItemAvgSql, feature);
            }
            else {
                feature = (Map<String, Map<String, Double>>) cache.get(userItemAvgSql);
            }
            log.info("trainPeriod: " + i + ", user item avg size: " + feature.size());
            mergeMap(userItemAvgFeature, feature);

            //////////////////////////////////////////////////////
            Map<String, Map<String, Double>> userPopular = null;
            if(null == cache.get("user_popular_" + i)){
                userPopular = Popular.userPopular(tablename, labelPeriod, i, "user_popular_");
                log.info("user popular size: " + userPopular.size());
                cache.put("user_popular_" + i, userPopular);
            }
            else {
                userPopular = (Map<String, Map<String, Double>>) cache.get("user_popular_" + i);
            }
            log.info("trainPeriod: " + i + ", user popular size: " + userPopular.size());
            mergeMap(userPopularMap, userPopular);

            Map<String, Map<String, Double>> itemPopular = null;
            if(null == cache.get("item_popular_" + i)){
                itemPopular = Popular.itemPopular(tablename, labelPeriod, i, "item_popular_");
                log.info("item popular size: " + itemPopular.size());
                cache.put("item_popular_" + i, itemPopular);
            }
            else {
                itemPopular = (Map<String, Map<String, Double>>) cache.get("item_popular_" + i);
            }
            log.info("trainPeriod: " + i + ", item popular size: " + itemPopular.size());
            mergeMap(itemPopularMap, itemPopular);

            Map<String, Map<String, Double>> itemActionUserCount = null;
            if(null == cache.get("item_action_user_" + i)){
                itemActionUserCount = Popular.itemActionUserCount(tablename, labelPeriod, i, "item_action_user_");
                log.info("item action user size: " + itemActionUserCount.size());
                cache.put("item_action_user_" + i, itemActionUserCount);
            }
            else {
                itemActionUserCount = (Map<String, Map<String, Double>>) cache.get("item_action_user_" + i);
            }
            log.info("trainPeriod: " + i + ", item action user size: " + itemActionUserCount.size());
            mergeMap(itemActionUserCountMap, itemActionUserCount);

            Map<String, Map<String, Double>> userItemClick = null;
            String userItemClickSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where click>0 and action_period=" + i + " group by user_id";
            if(null == cache.get(userItemClickSql)){
                userItemClick = ItemBuyUsers.itemUserCount(userItemClickSql, "user_id", labelPeriod, i, "user_distinct_item_click_");
                cache.put(userItemClickSql, userItemClick);
            }
            else {
                userItemClick = (Map<String, Map<String, Double>>) cache.get(userItemClickSql);
            }
            log.info("trainPeriod: " + i + ", user item click size: " + userItemClick.size());
            mergeMap(userItemClickMap, userItemClick);

            Map<String, Map<String, Double>> userItemDetail = null;
            String userItemDetailSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where detail>0 and action_period=" + i + " group by user_id";
            if(null == cache.get(userItemDetailSql)){
                userItemDetail = ItemBuyUsers.itemUserCount(userItemDetailSql, "user_id", labelPeriod, i, "user_distinct_item_detail_");
                cache.put(userItemDetailSql, userItemDetail);
            }
            else {
                userItemDetail = (Map<String, Map<String, Double>>) cache.get(userItemDetailSql);
            }
            log.info("trainPeriod: " + i + ", user item detail size: " + userItemDetail.size());
            mergeMap(userItemDetailMap, userItemDetail);

            Map<String, Map<String, Double>> userItemCart = null;
            String userItemCartSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where cart>0 and action_period=" + i + " group by user_id";
            if(null == cache.get(userItemCartSql)){
                userItemCart = ItemBuyUsers.itemUserCount(userItemCartSql, "user_id", labelPeriod, i, "user_distinct_item_cart_");
                cache.put(userItemCartSql, userItemCart);
            }
            else {
                userItemCart = (Map<String, Map<String, Double>>) cache.get(userItemCartSql);
            }
            log.info("trainPeriod: " + i + ", user item cart size: " + userItemCart.size());
            mergeMap(userItemCartMap, userItemCart);

            Map<String, Map<String, Double>> userItemCartDelete = null;
            String userItemCartDeleteSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where cart_delete>0 and action_period=" + i + " group by user_id";
            if(null == cache.get(userItemCartDeleteSql)){
                userItemCartDelete = ItemBuyUsers.itemUserCount(userItemCartDeleteSql, "user_id", labelPeriod, i, "user_distinct_item_cart_delete_");
                cache.put(userItemCartDeleteSql, userItemCartDelete);
            }
            else {
                userItemCartDelete = (Map<String, Map<String, Double>>) cache.get(userItemCartDeleteSql);
            }
            log.info("trainPeriod: " + i + ", user item cart delete size: " + userItemCartDelete.size());
            mergeMap(userItemCartDeleteMap, userItemCartDelete);

            Map<String, Map<String, Double>> userItemFollow = null;
            String userItemFollowSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from " + tablename + " where follow>0 and action_period=" + i + " group by user_id";
            if(null == cache.get(userItemFollowSql)){
                userItemFollow = ItemBuyUsers.itemUserCount(userItemFollowSql, "user_id", labelPeriod, i, "user_distinct_item_follow_");
                cache.put(userItemFollowSql, userItemFollow);
            }
            else {
                userItemFollow = (Map<String, Map<String, Double>>) cache.get(userItemFollowSql);
            }
            log.info("trainPeriod: " + i + ", user item follow size: " + userItemFollow.size());
            mergeMap(userItemFollowMap, userItemFollow);

            Map<String, Map<String, Double>> itemUserClick = null;
            String itemUserClickSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where click >0 and action_period=" + i + " group by sku_id";
            if(null == cache.get(itemUserClickSql)){
                itemUserClick = ItemBuyUsers.itemUserCount(itemUserClickSql, "sku_id", labelPeriod, i, "item_distinct_user_click_");
                cache.put(itemUserClickSql, itemUserClick);
            }
            else {
                itemUserClick = (Map<String, Map<String, Double>>) cache.get(itemUserClickSql);
            }
            log.info("trainPeriod: " + i + ", item user click size: " + itemUserClick.size());
            mergeMap(itemUserClickMap, itemUserClick);

            Map<String, Map<String, Double>> itemUserDetail = null;
            String itemUserDetailSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where detail>0 and action_period=" + i + " group by sku_id";
            if(null == cache.get(itemUserDetailSql)){
                itemUserDetail = ItemBuyUsers.itemUserCount(itemUserDetailSql, "sku_id", labelPeriod, i, "item_distinct_user_detail_");
                cache.put(itemUserDetailSql, itemUserDetail);
            }
            else {
                itemUserDetail = (Map<String, Map<String, Double>>) cache.get(itemUserDetailSql);
            }
            log.info("trainPeriod: " + i + ", item user detail size: " + itemUserDetail.size());
            mergeMap(itemUserDetailMap, itemUserDetail);

            Map<String, Map<String, Double>> itemUserCart = null;
            String itemUserCartSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where cart>0 and action_period=" + i + " group by sku_id";
            if(null == cache.get(itemUserCartSql)){
                itemUserCart = ItemBuyUsers.itemUserCount(itemUserCartSql, "sku_id", labelPeriod, i, "item_distinct_user_cart_");
                cache.put(itemUserCartSql, itemUserCart);
            }
            else {
                itemUserCart = (Map<String, Map<String, Double>>) cache.get(itemUserCartSql);
            }
            log.info("trainPeriod: " + i + ", item user cart size: " + itemUserCart.size());
            mergeMap(itemUserCartMap, itemUserCart);

            Map<String, Map<String, Double>> itemUserCartDelete = null;
            String itemUserCartDeleteSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where cart_delete>0 and action_period=" + i + " group by sku_id";
            if(null == cache.get(itemUserCartDeleteSql)){
                itemUserCartDelete = ItemBuyUsers.itemUserCount(itemUserCartDeleteSql, "sku_id", labelPeriod, i, "item_distinct_user_cart_delete_");
                cache.put(itemUserCartDeleteSql, itemUserCartDelete);
            }
            else {
                itemUserCartDelete = (Map<String, Map<String, Double>>) cache.get(itemUserCartDeleteSql);
            }
            log.info("trainPeriod: " + i + ", item user cart delete size: " + itemUserCartDelete.size());
            mergeMap(itemUserCartDeleteMap, itemUserCartDelete);

            Map<String, Map<String, Double>> itemUserFollow = null;
            String itemUserFollowSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from " + tablename + " where follow>0 and action_period=" + i + " group by sku_id";
            if(null == cache.get(itemUserFollowSql)){
                itemUserFollow = ItemBuyUsers.itemUserCount(itemUserFollowSql, "sku_id", labelPeriod, i, "item_distinct_user_follow_");
                cache.put(itemUserFollowSql, itemUserFollow);
            }
            else {
                itemUserFollow = (Map<String, Map<String, Double>>) cache.get(itemUserFollowSql);
            }
            log.info("trainPeriod: " + i + ", item user follow size: " + itemUserFollow.size());
            mergeMap(itemUserFollowMap, itemUserFollow);
        }

        Map<String, Long> labelMap = new HashMap<>();
        if (!isPredict) {
            String labelSql = "select user_id,sku_id,count(1) as count from " + tablename + " where buy>0 and action_period=" + labelPeriod + " group by user_id,sku_id";
            log.info("label sql: " + labelSql);
            List<Map<String, Object>> labelResult = null;
            if(null == cache.get(labelSql)){
                labelResult = DBOperation.queryBySql(labelSql);
                cache.put(labelSql, labelResult);
            }
            else {
                labelResult = (List<Map<String, Object>>) cache.get(labelSql);
            }
            log.info("label size: " + labelResult.size());
            for (Map<String, Object> row : labelResult) {
                String userId = String.valueOf(row.get("user_id"));
                String skuId = String.valueOf(row.get("sku_id"));
                long count = (long) row.get("count");

                labelMap.put(userId + "_" + skuId, count);
            }
        }

        String periods = getPeriod(trainStartPeriod, trainEndPeriod);

        int positive = 0;
        int negative = 0;
        String dataSql = "select user_id,sku_id,group_concat(model_id separator '_') as model_id from " + tablename + " where action_period in(" + periods + ") group by user_id,sku_id";
        log.info("data sql: " + dataSql);
        List<Map<String, Object>> infoResult = null;
        if(null == cache.get(dataSql)){
            infoResult = DBOperation.queryBySql(dataSql);
            cache.put(dataSql, infoResult);
        }
        else {
            infoResult = (List<Map<String, Object>>) cache.get(dataSql);
        }
        log.info("data size: " + infoResult.size());
        for (Map<String, Object> row : infoResult) {
            Map<String, Object> line = new HashMap<>();
            for (int modelId : modelIds) {
                line.put("model_" + modelId, 0);
            }

            String userId = String.valueOf(row.get("user_id"));
            String skuId = String.valueOf(row.get("sku_id"));

            Map<String, Object> userInfo = userInfos.get(userId);
            Map<String, Object> itemInfo = itemInfos.get(skuId);
            Map<String, Object> commentInfo = commentInfos.get(skuId);

            int age = (int) userInfo.get("age");
            int sex = (int) userInfo.get("sex");
            int userLevel = (int) userInfo.get("user_level");
            int attr1 = (int) itemInfo.get("attr1");
            int attr2 = (int) itemInfo.get("attr2");
            int attr3 = (int) itemInfo.get("attr3");
            int cate = (int) itemInfo.get("cate");
            int brand = (int) itemInfo.get("brand");

            int commentNum = 0;
            int hasBadComment = 0;
            double badCommentRate = 0d;
            if (null != commentInfo) {
                commentNum = (int) commentInfo.get("comment_num");
                hasBadComment = (int) commentInfo.get("has_bad_comment");
                badCommentRate = (double) commentInfo.get("bad_comment_rate");
            }

            line.put("user_id", userId);
            line.put("sku_id", skuId);
            line.put("age", age);
            line.put("sex", sex);
            line.put("user_level", userLevel);
            line.put("attr1", attr1);
            line.put("attr2", attr2);
            line.put("attr3", attr3);
            line.put("cate", cate);
            line.put("brand", brand);
            line.put("comment_num", commentNum);
            line.put("has_bad_comment", hasBadComment);
            line.put("bad_comment_rate", badCommentRate);

            Map<String, Integer> modelIdMap = new HashMap<>();
            String modelIdStr = (String) row.get("model_id");
            if (StringUtils.isNotBlank(modelIdStr)) {
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

                for (Map.Entry<String, Integer> entry : modelIdMap.entrySet()) {
                    line.put("model_" + entry.getKey(), entry.getValue());
                }
            }

            setFeature(line, userCountFeature.get(userId));
            setFeature(line, userSumFeature.get(userId));
            setFeature(line, userAvgFeature.get(userId));
            setFeature(line, itemCountFeature.get(skuId));
            setFeature(line, itemSumFeature.get(skuId));
            setFeature(line, itemAvgFeature.get(skuId));
            setFeature(line, userItemCountFeature.get(userId + "_" + skuId));
            setFeature(line, userItemSumFeature.get(userId + "_" + skuId));
            setFeature(line, userItemAvgFeature.get(userId + "_" + skuId));

            setFeature(line, userPopularMap.get(userId));
            setFeature(line, itemPopularMap.get(skuId));
            setFeature(line, itemActionUserCountMap.get(skuId));
            setFeature(line, userItemClickMap.get(userId));
            setFeature(line, userItemDetailMap.get(userId));
            setFeature(line, userItemCartMap.get(userId));
            setFeature(line, userItemCartDeleteMap.get(userId));
            setFeature(line, userItemFollowMap.get(userId));
            setFeature(line, itemUserClickMap.get(skuId));
            setFeature(line, itemUserDetailMap.get(skuId));
            setFeature(line, itemUserCartMap.get(skuId));
            setFeature(line, itemUserCartDeleteMap.get(skuId));
            setFeature(line, itemUserFollowMap.get(skuId));

            if (!isPredict) {
                Long count = labelMap.get(userId + "_" + skuId);
                if (null == count) {
                    line.put("target", 1);
                    ++negative;
                } else {
                    line.put("target", 0);
                    ++positive;
                }
            }

            lines.add(line);
        }

        log.info("positive: " + positive);
        log.info("negative: " + negative);

        List<String[]> allLines = new ArrayList<>();

        CSVWriter writer = new CSVWriter(new FileWriter(path + "/" + filename), ',', '\0');
        Map<String, Object> max = getMax(lines);
        writer.writeNext(max.keySet().toArray(new String[0]));
        for (Map<String, Object> line : lines) {
            String[] array = new String[max.keySet().size()];
            int idx = 0;
            for(String key : max.keySet()){
                array[idx] = String.valueOf(null == line.get(key) ? 0 : line.get(key));
                ++idx;
            }

            allLines.add(array);
//            writer.writeNext(array);
        }

        writer.writeAll(allLines);
        writer.close();
    }

    private static void setFeature(Map<String, Object> line, Map<String, Double> values){
        if(null != values){
            for(Entry<String, Double> entry : values.entrySet()){
                line.put(entry.getKey(), entry.getValue());
            }
        }
    }

    private static void mergeMap(Map<String, Map<String, Double>> totalFeatureMap, Map<String, Map<String, Double>> featureMap){
        for(Entry<String, Map<String, Double>> entry : featureMap.entrySet()){
            String key = entry.getKey();
            Map<String, Double> value = entry.getValue();
            if(totalFeatureMap.containsKey(key)){
                totalFeatureMap.get(key).putAll(value);
            }
            else {
                totalFeatureMap.put(key, value);
            }
        }
    }

    private static  String getPeriod(int start, int end){
        String s = "";
        for(int i = start; i <= end; i++){
            s += i + ",";
        }

        s = StringUtils.removeEnd(s, ",");

        return s;
    }

    private static Map<String, Object> getMax(List<Map<String, Object>> lines){
        Map<String, Object> max = lines.get(0);
        for(Map<String, Object> line : lines){
            if(line.size() > max.size()){
                max = line;
            }
        }

        return max;
    }
}

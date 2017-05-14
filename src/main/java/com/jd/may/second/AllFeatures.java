package com.jd.may.second;

import au.com.bytecode.opencsv.CSVWriter;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.util.*;
import java.util.Map.Entry;

/**
 * Created by wanghl on 17-5-7.
 */
public class AllFeatures {
    private static final Logger log = Logger.getLogger(AllFeatures.class);
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

        attributes.put("user_click_count_rank", i++);
        attributes.put("user_detail_count_rank", i++);
        attributes.put("user_cart_count_rank", i++);
        attributes.put("user_cart_delete_count_rank", i++);
        attributes.put("user_buy_count_rank", i++);
        attributes.put("user_follow_count_rank", i++);
        attributes.put("user_click_sum_rank", i++);
        attributes.put("user_detail_sum_rank", i++);
        attributes.put("user_cart_sum_rank", i++);
        attributes.put("user_cart_delete_sum_rank", i++);
        attributes.put("user_buy_sum_rank", i++);
        attributes.put("user_follow_sum_rank", i++);
        attributes.put("user_click_avg_rank", i++);
        attributes.put("user_detail_avg_rank", i++);
        attributes.put("user_cart_avg_rank", i++);
        attributes.put("user_cart_delete_avg_rank", i++);
        attributes.put("user_buy_avg_rank", i++);
        attributes.put("user_follow_avg_rank", i++);
        attributes.put("user_buy_click_ratio_rank", i++);
        attributes.put("user_buy_detail_ratio_rank", i++);
        attributes.put("user_buy_cart_ratio_rank", i++);
        attributes.put("user_buy_cart_delete_ratio_rank", i++);
        attributes.put("user_buy_follow_ratio_rank", i++);

        attributes.put("item_click_count_rank", i++);
        attributes.put("item_detail_count_rank", i++);
        attributes.put("item_cart_count_rank", i++);
        attributes.put("item_cart_delete_count_rank", i++);
        attributes.put("item_buy_count_rank", i++);
        attributes.put("item_follow_count_rank", i++);
        attributes.put("item_click_sum_rank", i++);
        attributes.put("item_detail_sum_rank", i++);
        attributes.put("item_cart_sum_rank", i++);
        attributes.put("item_cart_delete_sum_rank", i++);
        attributes.put("item_buy_sum_rank", i++);
        attributes.put("item_follow_sum_rank", i++);
        attributes.put("item_click_avg_rank", i++);
        attributes.put("item_detail_avg_rank", i++);
        attributes.put("item_cart_avg_rank", i++);
        attributes.put("item_cart_delete_avg_rank", i++);
        attributes.put("item_buy_avg_rank", i++);
        attributes.put("item_follow_avg_rank", i++);
        attributes.put("item_buy_click_ratio_rank", i++);
        attributes.put("item_buy_detail_ratio_rank", i++);
        attributes.put("item_buy_cart_ratio_rank", i++);
        attributes.put("item_buy_cart_delete_ratio_rank", i++);
        attributes.put("item_buy_follow_ratio_rank", i++);

        attributes.put("user_item_click_count_rank", i++);
        attributes.put("user_item_detail_count_rank", i++);
        attributes.put("user_item_cart_count_rank", i++);
        attributes.put("user_item_cart_delete_count_rank", i++);
        attributes.put("user_item_buy_count_rank", i++);
        attributes.put("user_item_follow_count_rank", i++);
        attributes.put("user_item_click_sum_rank", i++);
        attributes.put("user_item_detail_sum_rank", i++);
        attributes.put("user_item_cart_sum_rank", i++);
        attributes.put("user_item_cart_delete_sum_rank", i++);
        attributes.put("user_item_buy_sum_rank", i++);
        attributes.put("user_item_follow_sum_rank", i++);
        attributes.put("user_item_click_avg_rank", i++);
        attributes.put("user_item_detail_avg_rank", i++);
        attributes.put("user_item_cart_avg_rank", i++);
        attributes.put("user_item_cart_delete_avg_rank", i++);
        attributes.put("user_item_buy_avg_rank", i++);
        attributes.put("user_item_follow_avg_rank", i++);
        attributes.put("user_item_buy_click_ratio_rank", i++);
        attributes.put("user_item_buy_detail_ratio_rank", i++);
        attributes.put("user_item_buy_cart_ratio_rank", i++);
        attributes.put("user_item_buy_cart_delete_ratio_rank", i++);
        attributes.put("user_item_buy_follow_ratio_rank", i++);

        attributes.put("user_popular", i++);
        attributes.put("user_popular_rank", i++);
        attributes.put("item_popular", i++);
        attributes.put("item_popular_rank", i++);

        attributes.put("item_action_user", i++);
        attributes.put("item_action_user_rank", i++);

        attributes.put("user_is_buy", i++);
        attributes.put("item_is_buy", i++);
        attributes.put("item_buy_dates", i++);

        attributes.put("user_distinct_item_click", i++);
        attributes.put("user_distinct_item_detail", i++);
        attributes.put("user_distinct_item_cart", i++);
        attributes.put("user_distinct_item_cart_delete", i++);
        attributes.put("user_distinct_item_buy", i++);
        attributes.put("user_distinct_item_follow", i++);
        attributes.put("user_distinct_item_click_rank", i++);
        attributes.put("user_distinct_item_detail_rank", i++);
        attributes.put("user_distinct_item_cart_rank", i++);
        attributes.put("user_distinct_item_cart_delete_rank", i++);
        attributes.put("user_distinct_item_buy_rank", i++);
        attributes.put("user_distinct_item_follow_rank", i++);

        attributes.put("item_distinct_user_click", i++);
        attributes.put("item_distinct_user_detail", i++);
        attributes.put("item_distinct_user_cart", i++);
        attributes.put("item_distinct_user_cart_delete", i++);
        attributes.put("item_distinct_user_buy", i++);
        attributes.put("item_distinct_user_follow", i++);
        attributes.put("item_distinct_user_click_rank", i++);
        attributes.put("item_distinct_user_detail_rank", i++);
        attributes.put("item_distinct_user_cart_rank", i++);
        attributes.put("item_distinct_user_cart_delete_rank", i++);
        attributes.put("item_distinct_user_buy_rank", i++);
        attributes.put("item_distinct_user_follow_rank", i++);

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

        attributes.put("user_ratio_user_click_cross_rank", i++);
        attributes.put("user_ratio_user_detail_cross_rank", i++);
        attributes.put("user_ratio_user_cart_cross_rank", i++);
        attributes.put("user_ratio_user_cart_delete_cross_rank", i++);
        attributes.put("user_ratio_user_follow_cross_rank", i++);
        attributes.put("user_ratio_user_item_click_cross_rank", i++);
        attributes.put("user_ratio_user_item_detail_cross_rank", i++);
        attributes.put("user_ratio_user_item_cart_cross_rank", i++);
        attributes.put("user_ratio_user_item_cart_delete_cross_rank", i++);
        attributes.put("user_ratio_user_item_follow_cross_rank", i++);

        attributes.put("item_ratio_item_click_cross_rank", i++);
        attributes.put("item_ratio_item_detail_cross_rank", i++);
        attributes.put("item_ratio_item_cart_cross_rank", i++);
        attributes.put("item_ratio_item_cart_delete_cross_rank", i++);
        attributes.put("item_ratio_item_follow_cross_rank", i++);
        attributes.put("item_ratio_user_item_click_cross_rank", i++);
        attributes.put("item_ratio_user_item_detail_cross_rank", i++);
        attributes.put("item_ratio_user_item_cart_cross_rank", i++);
        attributes.put("item_ratio_user_item_cart_delete_cross_rank", i++);
        attributes.put("item_ratio_user_item_follow_cross_rank", i++);

        attributes.put("user_item_ratio_user_click_cross_rank", i++);
        attributes.put("user_item_ratio_user_detail_cross_rank", i++);
        attributes.put("user_item_ratio_user_cart_cross_rank", i++);
        attributes.put("user_item_ratio_user_cart_delete_cross_rank", i++);
        attributes.put("user_item_ratio_user_follow_cross_rank", i++);
        attributes.put("user_item_ratio_item_click_cross_rank", i++);
        attributes.put("user_item_ratio_item_detail_cross_rank", i++);
        attributes.put("user_item_ratio_item_cart_cross_rank", i++);
        attributes.put("user_item_ratio_item_cart_delete_cross_rank", i++);
        attributes.put("user_item_ratio_item_follow_cross_rank", i++);
        attributes.put("user_item_ratio_user_item_click_cross_rank", i++);
        attributes.put("user_item_ratio_user_item_detail_cross_rank", i++);
        attributes.put("user_item_ratio_user_item_cart_cross_rank", i++);
        attributes.put("user_item_ratio_user_item_cart_delete_cross_rank", i++);
        attributes.put("user_item_ratio_user_item_follow_cross_rank", i++);

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

        Map<String, Integer> userClickCountRank = Rank.rank(userCountFeature, "click");
        Map<String, Integer> userDetailCountRank = Rank.rank(userCountFeature, "detail");
        Map<String, Integer> userCartCountRank = Rank.rank(userCountFeature, "cart");
        Map<String, Integer> userCartDeleteCountRank = Rank.rank(userCountFeature, "cartDelete");
        Map<String, Integer> userBuyCountRank = Rank.rank(userCountFeature, "buy");
        Map<String, Integer> userFollowCountRank = Rank.rank(userCountFeature, "follow");
        Map<String, Integer> userClickSumRank = Rank.rank(userSumFeature, "click");
        Map<String, Integer> userDetailSumRank = Rank.rank(userSumFeature, "detail");
        Map<String, Integer> userCartSumRank = Rank.rank(userSumFeature, "cart");
        Map<String, Integer> userCartDeleteSumRank = Rank.rank(userSumFeature, "cartDelete");
        Map<String, Integer> userBuySumRank = Rank.rank(userSumFeature, "buy");
        Map<String, Integer> userFollowSumRank = Rank.rank(userSumFeature, "follow");
        Map<String, Integer> userClickAvgRank = Rank.rank(userAvgFeature, "click");
        Map<String, Integer> userDetailAvgRank = Rank.rank(userAvgFeature, "detail");
        Map<String, Integer> userCartAvgRank = Rank.rank(userAvgFeature, "cart");
        Map<String, Integer> userCartDeleteAvgRank = Rank.rank(userAvgFeature, "cartDelete");
        Map<String, Integer> userBuyAvgRank = Rank.rank(userAvgFeature, "buy");
        Map<String, Integer> userFollowAvgRank = Rank.rank(userAvgFeature, "follow");
        Map<String, Integer> userBuyClickRatioRank = Rank.rank(userBuyRatioFeature, "click");
        Map<String, Integer> userBuyDetailRatioRank = Rank.rank(userBuyRatioFeature, "detail");
        Map<String, Integer> userBuyCartRatioRank = Rank.rank(userBuyRatioFeature, "cart");
        Map<String, Integer> userBuyCartDeleteRatioRank = Rank.rank(userBuyRatioFeature, "cartDelete");
        Map<String, Integer> userBuyFollowRatioRank = Rank.rank(userBuyRatioFeature, "follow");
        Map<String, Integer> itemClickCountRank = Rank.rank(itemCountFeature, "click");
        Map<String, Integer> itemDetailCountRank = Rank.rank(itemCountFeature, "detail");
        Map<String, Integer> itemCartCountRank = Rank.rank(itemCountFeature, "cart");
        Map<String, Integer> itemCartDeleteCountRank = Rank.rank(itemCountFeature, "cartDelete");
        Map<String, Integer> itemBuyCountRank = Rank.rank(itemCountFeature, "buy");
        Map<String, Integer> itemFollowCountRank = Rank.rank(itemCountFeature, "follow");
        Map<String, Integer> itemClickSumRank = Rank.rank(itemSumFeature, "click");
        Map<String, Integer> itemDetailSumRank = Rank.rank(itemSumFeature, "detail");
        Map<String, Integer> itemCartSumRank = Rank.rank(itemSumFeature, "cart");
        Map<String, Integer> itemCartDeleteSumRank = Rank.rank(itemSumFeature, "cartDelete");
        Map<String, Integer> itemBuySumRank = Rank.rank(itemSumFeature, "buy");
        Map<String, Integer> itemFollowSumRank = Rank.rank(itemSumFeature, "follow");
        Map<String, Integer> itemClickAvgRank = Rank.rank(itemAvgFeature, "click");
        Map<String, Integer> itemDetailAvgRank = Rank.rank(itemAvgFeature, "detail");
        Map<String, Integer> itemCartAvgRank = Rank.rank(itemAvgFeature, "cart");
        Map<String, Integer> itemCartDeleteAvgRank = Rank.rank(itemAvgFeature, "cartDelete");
        Map<String, Integer> itemBuyAvgRank = Rank.rank(itemAvgFeature, "buy");
        Map<String, Integer> itemFollowAvgRank = Rank.rank(itemAvgFeature, "follow");
        Map<String, Integer> itemBuyClickRatioRank = Rank.rank(itemBuyRatioFeature, "click");
        Map<String, Integer> itemBuyDetailRatioRank = Rank.rank(itemBuyRatioFeature, "detail");
        Map<String, Integer> itemBuyCartRatioRank = Rank.rank(itemBuyRatioFeature, "cart");
        Map<String, Integer> itemBuyCartDeleteRatioRank = Rank.rank(itemBuyRatioFeature, "cartDelete");
        Map<String, Integer> itemBuyFollowRatioRank = Rank.rank(itemBuyRatioFeature, "follow");
        Map<String, Integer> userItemClickCountRank = Rank.rank(userItemCountFeature, "click");
        Map<String, Integer> userItemDetailCountRank = Rank.rank(userItemCountFeature, "detail");
        Map<String, Integer> userItemCartCountRank = Rank.rank(userItemCountFeature, "cart");
        Map<String, Integer> userItemCartDeleteCountRank = Rank.rank(userItemCountFeature, "cartDelete");
        Map<String, Integer> userItemBuyCountRank = Rank.rank(userItemCountFeature, "buy");
        Map<String, Integer> userItemFollowCountRank = Rank.rank(userItemCountFeature, "follow");
        Map<String, Integer> userItemClickSumRank = Rank.rank(userItemSumFeature, "click");
        Map<String, Integer> userItemDetailSumRank = Rank.rank(userItemSumFeature, "detail");
        Map<String, Integer> userItemCartSumRank = Rank.rank(userItemSumFeature, "cart");
        Map<String, Integer> userItemCartDeleteSumRank = Rank.rank(userItemSumFeature, "cartDelete");
        Map<String, Integer> userItemBuySumRank = Rank.rank(userItemSumFeature, "buy");
        Map<String, Integer> userItemFollowSumRank = Rank.rank(userItemSumFeature, "follow");
        Map<String, Integer> userItemClickAvgRank = Rank.rank(userItemAvgFeature, "click");
        Map<String, Integer> userItemDetailAvgRank = Rank.rank(userItemAvgFeature, "detail");
        Map<String, Integer> userItemCartAvgRank = Rank.rank(userItemAvgFeature, "cart");
        Map<String, Integer> userItemCartDeleteAvgRank = Rank.rank(userItemAvgFeature, "cartDelete");
        Map<String, Integer> userItemBuyAvgRank = Rank.rank(userItemAvgFeature, "buy");
        Map<String, Integer> userItemFollowAvgRank = Rank.rank(userItemAvgFeature, "follow");
        Map<String, Integer> userItemBuyClickRatioRank = Rank.rank(userItemBuyRatioFeature, "click");
        Map<String, Integer> userItemBuyDetailRatioRank = Rank.rank(userItemBuyRatioFeature, "detail");
        Map<String, Integer> userItemBuyCartRatioRank = Rank.rank(userItemBuyRatioFeature, "cart");
        Map<String, Integer> userItemBuyCartDeleteRatioRank = Rank.rank(userItemBuyRatioFeature, "cartDelete");
        Map<String, Integer> userItemBuyFollowRatioRank = Rank.rank(userItemBuyRatioFeature, "follow");

        Map<String, Double> userPopular = Popular.userPopular(start, end);
        Map<String, Integer> userPopularRank = Rank.rank(userPopular);
        Map<String, Double> itemPopular = Popular.itemPopular(start, end);
        Map<String, Integer> itemPopularRank = Rank.rank(itemPopular);


        Map<String, Double> itemActionUserCount = Popular.itemActionUserCount(start, end);
        Map<String, Integer> itemActionUserRank = Rank.rank(itemActionUserCount);

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
        Map<String, Integer> userItemClickRank = Rank.rank(userItemClick);
        Map<String, Integer> userItemDetailRank = Rank.rank(userItemDetail);
        Map<String, Integer> userItemCartRank = Rank.rank(userItemCart);
        Map<String, Integer> userItemCartDeleteRank = Rank.rank(userItemCartDelete);
        Map<String, Integer> userItemBuyRank = Rank.rank(userItemBuy);
        Map<String, Integer> userItemFollowRank = Rank.rank(userItemFollow);

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
        Map<String, Integer> itemUserClickRank = Rank.rank(itemUserClick);
        Map<String, Integer> itemUserDetailRank = Rank.rank(itemUserDetail);
        Map<String, Integer> itemUserCartRank = Rank.rank(itemUserCart);
        Map<String, Integer> itemUserCartDeleteRank = Rank.rank(itemUserCartDelete);
        Map<String, Integer> itemUserBuyRank = Rank.rank(itemUserBuy);
        Map<String, Integer> itemUserFollowRank = Rank.rank(itemUserFollow);

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

        for(Entry<String, Map<String, Double>> entry : userBuyRatioFeature.entrySet()){
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
            for(Entry<String, Map<String, Double>> ent : userMap.entrySet()){
                String key = ent.getKey();
                Map<String, Double> sumMap = ent.getValue();
                userRatioUserItemClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == sumMap.get("click") ? 0 : sumMap.get("click"))));
                userRatioUserItemDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == sumMap.get("detail") ? 0 : sumMap.get("detail"))));
                userRatioUserItemCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == sumMap.get("cart") ? 0 : sumMap.get("cart"))));
                userRatioUserItemCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == sumMap.get("cartDelete") ? 0 : sumMap.get("cartDelete"))));
                userRatioUserItemFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == sumMap.get("follow") ? 0 : sumMap.get("follow"))));
            }
        }

        for(Entry<String, Map<String, Double>> entry : itemBuyRatioFeature.entrySet()){
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
            for(Entry<String, Map<String, Double>> ent : itemMap.entrySet()) {
                String key = ent.getKey();
                Map<String, Double> sumMap = ent.getValue();
                itemRatioUserItemClickCross.put(key, DigitalFormat.formatForDouble(buyClickRatio * (null == sumMap.get("click") ? 0 : sumMap.get("click"))));
                itemRatioUserItemDetailCross.put(key, DigitalFormat.formatForDouble(buyDetailRatio * (null == sumMap.get("detail") ? 0 : sumMap.get("detail"))));
                itemRatioUserItemCartCross.put(key, DigitalFormat.formatForDouble(buyCartRatio * (null == sumMap.get("cart") ? 0 : sumMap.get("cart"))));
                itemRatioUserItemCartDeleteCross.put(key, DigitalFormat.formatForDouble(buyCartDeleteRatio * (null == sumMap.get("cartDelete") ? 0 : sumMap.get("cartDelete"))));
                itemRatioUserItemFollowCross.put(key, DigitalFormat.formatForDouble(buyFollowRatio * (null == sumMap.get("follow") ? 0 : sumMap.get("follow"))));
            }
        }

        for(Entry<String, Map<String, Double>> entry : userItemBuyRatioFeature.entrySet()){
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

        Map<String, Integer> userRatioUserClickCrossRank = Rank.rank(userRatioUserClickCross);
        Map<String, Integer> userRatioUserDetailCrossRank = Rank.rank(userRatioUserDetailCross);
        Map<String, Integer> userRatioUserCartCrossRank = Rank.rank(userRatioUserCartCross);
        Map<String, Integer> userRatioUserCartDeleteCrossRank = Rank.rank(userRatioUserCartDeleteCross);
        Map<String, Integer> userRatioUserFollowCrossRank = Rank.rank(userRatioUserFollowCross);
        Map<String, Integer> userRatioUserItemClickCrossRank = Rank.rank(userRatioUserItemClickCross);
        Map<String, Integer> userRatioUserItemDetailCrossRank = Rank.rank(userRatioUserItemDetailCross);
        Map<String, Integer> userRatioUserItemCartCrossRank = Rank.rank(userRatioUserItemCartCross);
        Map<String, Integer> userRatioUserItemCartDeleteCrossRank = Rank.rank(userRatioUserItemCartDeleteCross);
        Map<String, Integer> userRatioUserItemFollowCrossRank = Rank.rank(userRatioUserItemFollowCross);

        Map<String, Integer> itemRatioItemClickCrossRank = Rank.rank(itemRatioItemClickCross);
        Map<String, Integer> itemRatioItemDetailCrossRank = Rank.rank(itemRatioItemDetailCross);
        Map<String, Integer> itemRatioItemCartCrossRank = Rank.rank(itemRatioItemCartCross);
        Map<String, Integer> itemRatioItemCartDeleteCrossRank = Rank.rank(itemRatioItemCartDeleteCross);
        Map<String, Integer> itemRatioItemFollowCrossRank = Rank.rank(itemRatioItemFollowCross);
        Map<String, Integer> itemRatioUserItemClickCrossRank = Rank.rank(itemRatioUserItemClickCross);
        Map<String, Integer> itemRatioUserItemDetailCrossRank = Rank.rank(itemRatioUserItemDetailCross);
        Map<String, Integer> itemRatioUserItemCartCrossRank = Rank.rank(itemRatioUserItemCartCross);
        Map<String, Integer> itemRatioUserItemCartDeleteCrossRank = Rank.rank(itemRatioUserItemCartDeleteCross);
        Map<String, Integer> itemRatioUserItemFollowCrossRank = Rank.rank(itemRatioUserItemFollowCross);

        Map<String, Integer> userItemRatioUserClickCrossRank = Rank.rank(userItemRatioUserClickCross);
        Map<String, Integer> userItemRatioUserDetailCrossRank = Rank.rank(userItemRatioUserDetailCross);
        Map<String, Integer> userItemRatioUserCartCrossRank = Rank.rank(userItemRatioUserCartCross);
        Map<String, Integer> userItemRatioUserCartDeleteCrossRank = Rank.rank(userItemRatioUserCartDeleteCross);
        Map<String, Integer> userItemRatioUserFollowCrossRank = Rank.rank(userItemRatioUserFollowCross);
        Map<String, Integer> userItemRatioItemClickCrossRank = Rank.rank(userItemRatioItemClickCross);
        Map<String, Integer> userItemRatioItemDetailCrossRank = Rank.rank(userItemRatioItemDetailCross);
        Map<String, Integer> userItemRatioItemCartCrossRank = Rank.rank(userItemRatioItemCartCross);
        Map<String, Integer> userItemRatioItemCartDeleteCrossRank = Rank.rank(userItemRatioItemCartDeleteCross);
        Map<String, Integer> userItemRatioItemFollowCrossRank = Rank.rank(userItemRatioItemFollowCross);
        Map<String, Integer> userItemRatioUserItemClickCrossRank = Rank.rank(userItemRatioUserItemClickCross);
        Map<String, Integer> userItemRatioUserItemDetailCrossRank = Rank.rank(userItemRatioUserItemDetailCross);
        Map<String, Integer> userItemRatioUserItemCartCrossRank = Rank.rank(userItemRatioUserItemCartCross);
        Map<String, Integer> userItemRatioUserItemCartDeleteCrossRank = Rank.rank(userItemRatioUserItemCartDeleteCross);
        Map<String, Integer> userItemRatioUserItemFollowCrossRank = Rank.rank(userItemRatioUserItemFollowCross);

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

            features[172] = String.valueOf(null == userClickCountRank.get(userId) ? 0 : userClickCountRank.get(userId));
            features[173] = String.valueOf(null == userDetailCountRank.get(userId) ? 0 : userDetailCountRank.get(userId));
            features[174] = String.valueOf(null == userCartCountRank.get(userId) ? 0 : userCartCountRank.get(userId));
            features[175] = String.valueOf(null == userCartDeleteCountRank.get(userId) ? 0 : userCartDeleteCountRank.get(userId));
            features[176] = String.valueOf(null == userBuyCountRank.get(userId) ? 0 : userBuyCountRank.get(userId));
            features[177] = String.valueOf(null == userFollowCountRank.get(userId) ? 0 : userFollowCountRank.get(userId));
            features[178] = String.valueOf(null == userClickSumRank.get(userId) ? 0 : userClickSumRank.get(userId));
            features[179] = String.valueOf(null == userDetailSumRank.get(userId) ? 0 : userDetailSumRank.get(userId));
            features[180] = String.valueOf(null == userCartSumRank.get(userId) ? 0 : userCartSumRank.get(userId));
            features[181] = String.valueOf(null == userCartDeleteSumRank.get(userId) ? 0 : userCartDeleteSumRank.get(userId));
            features[182] = String.valueOf(null == userBuySumRank.get(userId) ? 0 : userBuySumRank.get(userId));
            features[183] = String.valueOf(null == userFollowSumRank.get(userId) ? 0 : userFollowSumRank.get(userId));
            features[184] = String.valueOf(null == userClickAvgRank.get(userId) ? 0 : userClickAvgRank.get(userId));
            features[185] = String.valueOf(null == userDetailAvgRank.get(userId) ? 0 : userDetailAvgRank.get(userId));
            features[186] = String.valueOf(null == userCartAvgRank.get(userId) ? 0 : userCartAvgRank.get(userId));
            features[187] = String.valueOf(null == userCartDeleteAvgRank.get(userId) ? 0 : userCartDeleteAvgRank.get(userId));
            features[188] = String.valueOf(null == userBuyAvgRank.get(userId) ? 0 : userBuyAvgRank.get(userId));
            features[189] = String.valueOf(null == userFollowAvgRank.get(userId) ? 0 : userFollowAvgRank.get(userId));
            features[190] = String.valueOf(null == userBuyClickRatioRank.get(userId) ? 0 : userBuyClickRatioRank.get(userId));
            features[191] = String.valueOf(null == userBuyDetailRatioRank.get(userId) ? 0 : userBuyDetailRatioRank.get(userId));
            features[192] = String.valueOf(null == userBuyCartRatioRank.get(userId) ? 0 : userBuyCartRatioRank.get(userId));
            features[193] = String.valueOf(null == userBuyCartDeleteRatioRank.get(userId) ? 0 : userBuyCartDeleteRatioRank.get(userId));
            features[194] = String.valueOf(null == userBuyFollowRatioRank.get(userId)? 0 : userBuyFollowRatioRank.get(userId));
            features[195] = String.valueOf(null == itemClickCountRank.get(skuId) ? 0 : itemClickCountRank.get(skuId));
            features[196] = String.valueOf(null == itemDetailCountRank.get(skuId) ? 0 : itemDetailCountRank.get(skuId));
            features[197] = String.valueOf(null == itemCartCountRank.get(skuId) ? 0 : itemCartCountRank.get(skuId));
            features[198] = String.valueOf(null == itemCartDeleteCountRank.get(skuId) ? 0 : itemCartDeleteCountRank.get(skuId));
            features[199] = String.valueOf(null == itemBuyCountRank.get(skuId) ? 0 : itemBuyCountRank.get(skuId));
            features[200] = String.valueOf(null == itemFollowCountRank.get(skuId) ? 0 : itemFollowCountRank.get(skuId));
            features[201] = String.valueOf(null == itemClickSumRank.get(skuId) ? 0 : itemClickSumRank.get(skuId));
            features[202] = String.valueOf(null == itemDetailSumRank.get(skuId) ? 0 : itemDetailSumRank.get(skuId));
            features[203] = String.valueOf(null == itemCartSumRank.get(skuId) ? 0 : itemCartSumRank.get(skuId));
            features[204] = String.valueOf(null == itemCartDeleteSumRank.get(skuId) ? 0 : itemCartDeleteSumRank.get(skuId));
            features[205] = String.valueOf(null == itemBuySumRank.get(skuId) ? 0 : itemBuySumRank.get(skuId));
            features[206] = String.valueOf(null == itemFollowSumRank.get(skuId) ? 0 : itemFollowSumRank.get(skuId));
            features[207] = String.valueOf(null == itemClickAvgRank.get(skuId) ? 0 : itemClickAvgRank.get(skuId));
            features[208] = String.valueOf(null == itemDetailAvgRank.get(skuId) ? 0 : itemDetailAvgRank.get(skuId));
            features[209] = String.valueOf(null == itemCartAvgRank.get(skuId) ? 0 : itemCartAvgRank.get(skuId));
            features[210] = String.valueOf(null == itemCartDeleteAvgRank.get(skuId) ? 0 : itemCartDeleteAvgRank.get(skuId));
            features[211] = String.valueOf(null == itemBuyAvgRank.get(skuId) ? 0 : itemBuyAvgRank.get(skuId));
            features[212] = String.valueOf(null == itemFollowAvgRank.get(skuId) ? 0 : itemFollowAvgRank.get(skuId));
            features[213] = String.valueOf(null == itemBuyClickRatioRank.get(skuId) ? 0 : itemBuyClickRatioRank.get(skuId));
            features[214] = String.valueOf(null == itemBuyDetailRatioRank.get(skuId) ? 0 : itemBuyDetailRatioRank.get(skuId));
            features[215] = String.valueOf(null == itemBuyCartRatioRank.get(skuId) ? 0 : itemBuyCartRatioRank.get(skuId));
            features[216] = String.valueOf(null == itemBuyCartDeleteRatioRank.get(skuId) ? 0 : itemBuyCartDeleteRatioRank.get(skuId));
            features[217] = String.valueOf(null == itemBuyFollowRatioRank.get(skuId) ? 0 : itemBuyFollowRatioRank.get(skuId));
            features[218] = String.valueOf(null == userItemClickCountRank.get(userId + "_" + skuId) ? 0 : userItemClickCountRank.get(userId + "_" + skuId));
            features[219] = String.valueOf(null == userItemDetailCountRank.get(userId + "_" + skuId) ? 0 : userItemDetailCountRank.get(userId + "_" + skuId));
            features[220] = String.valueOf(null == userItemCartCountRank.get(userId + "_" + skuId) ? 0 : userItemCartCountRank.get(userId + "_" + skuId));
            features[221] = String.valueOf(null == userItemCartDeleteCountRank.get(userId + "_" + skuId) ? 0 : userItemCartDeleteCountRank.get(userId + "_" + skuId));
            features[222] = String.valueOf(null == userItemBuyCountRank.get(userId + "_" + skuId) ? 0 : userItemBuyCountRank.get(userId + "_" + skuId));
            features[223] = String.valueOf(null == userItemFollowCountRank.get(userId + "_" + skuId) ? 0 : userItemFollowCountRank.get(userId + "_" + skuId));
            features[224] = String.valueOf(null == userItemClickSumRank.get(userId + "_" + skuId) ? 0 : userItemClickSumRank.get(userId + "_" + skuId));
            features[225] = String.valueOf(null == userItemDetailSumRank.get(userId + "_" + skuId) ? 0 : userItemDetailSumRank.get(userId + "_" + skuId));
            features[226] = String.valueOf(null == userItemCartSumRank.get(userId + "_" + skuId) ? 0 : userItemCartSumRank.get(userId + "_" + skuId));
            features[227] = String.valueOf(null == userItemCartDeleteSumRank.get(userId + "_" + skuId) ? 0 : userItemCartDeleteSumRank.get(userId + "_" + skuId));
            features[228] = String.valueOf(null == userItemBuySumRank.get(userId + "_" + skuId) ? 0 : userItemBuySumRank.get(userId + "_" + skuId));
            features[229] = String.valueOf(null == userItemFollowSumRank.get(userId + "_" + skuId) ? 0 : userItemFollowSumRank.get(userId + "_" + skuId));
            features[230] = String.valueOf(null == userItemClickAvgRank.get(userId + "_" + skuId) ? 0 : userItemClickAvgRank.get(userId + "_" + skuId));
            features[231] = String.valueOf(null == userItemDetailAvgRank.get(userId + "_" + skuId) ? 0 : userItemDetailAvgRank.get(userId + "_" + skuId));
            features[232] = String.valueOf(null == userItemCartAvgRank.get(userId + "_" + skuId) ? 0 : userItemCartAvgRank.get(userId + "_" + skuId));
            features[233] = String.valueOf(null == userItemCartDeleteAvgRank.get(userId + "_" + skuId) ? 0 : userItemCartDeleteAvgRank.get(userId + "_" + skuId));
            features[234] = String.valueOf(null == userItemBuyAvgRank.get(userId + "_" + skuId) ? 0 : userItemBuyAvgRank.get(userId + "_" + skuId));
            features[235] = String.valueOf(null == userItemFollowAvgRank.get(userId + "_" + skuId) ? 0 : userItemFollowAvgRank.get(userId + "_" + skuId));
            features[236] = String.valueOf(null == userItemBuyClickRatioRank.get(userId + "_" + skuId) ? 0 : userItemBuyClickRatioRank.get(userId + "_" + skuId));
            features[237] = String.valueOf(null == userItemBuyDetailRatioRank.get(userId + "_" + skuId) ? 0 : userItemBuyDetailRatioRank.get(userId + "_" + skuId));
            features[238] = String.valueOf(null == userItemBuyCartRatioRank.get(userId + "_" + skuId) ? 0 : userItemBuyCartRatioRank.get(userId + "_" + skuId));
            features[239] = String.valueOf(null == userItemBuyCartDeleteRatioRank.get(userId + "_" + skuId) ? 0 : userItemBuyCartDeleteRatioRank.get(userId + "_" + skuId));
            features[240] = String.valueOf(null == userItemBuyFollowRatioRank.get(userId + "_" + skuId) ? 0 : userItemBuyFollowRatioRank.get(userId + "_" + skuId));

            features[241] = String.valueOf(null == userPopular.get(userId) ? 0 : userPopular.get(userId));
            features[242] = String.valueOf(null == userPopularRank.get(userId) ? 0 : userPopularRank.get(userId));
            features[243] = String.valueOf(null == itemPopular.get(skuId) ? 0 : itemPopular.get(skuId));
            features[244] = String.valueOf(null == itemPopularRank.get(skuId) ? 0 : itemPopularRank.get(skuId));
            features[245] = String.valueOf(null == itemActionUserCount.get(skuId) ? 0 : itemActionUserCount.get(skuId));
            features[246] = String.valueOf(null == itemActionUserRank.get(skuId) ? 0 : itemActionUserRank.get(skuId));

            features[247] = String.valueOf(null == userIsBuy.get(userId) ? 0 : userIsBuy.get(userId));
            features[248] = String.valueOf(null == itemIsBuy.get(skuId) ? 0 : itemIsBuy.get(skuId));
            features[249] = String.valueOf(null == itemBuyDate.get(skuId) ? 0 : itemBuyDate.get(skuId));

            features[250] = String.valueOf(null == userItemClick.get(userId) ? 0 : userItemClick.get(userId));
            features[251] = String.valueOf(null == userItemDetail.get(userId) ? 0 : userItemDetail.get(userId));
            features[252] = String.valueOf(null == userItemCart.get(userId) ? 0 : userItemCart.get(userId));
            features[253] = String.valueOf(null == userItemCartDelete.get(userId) ? 0 : userItemCartDelete.get(userId));
            features[254] = String.valueOf(null == userItemBuy.get(userId) ? 0 : userItemBuy.get(userId));
            features[255] = String.valueOf(null == userItemFollow.get(userId) ? 0 : userItemFollow.get(userId));

            features[256] = String.valueOf(null == userItemClickRank.get(userId) ? 0 : userItemClickRank.get(userId));
            features[257] = String.valueOf(null == userItemDetailRank.get(userId) ? 0 : userItemDetailRank.get(userId));
            features[258] = String.valueOf(null == userItemCartRank.get(userId) ? 0 : userItemCartRank.get(userId));
            features[259] = String.valueOf(null == userItemCartDeleteRank.get(userId) ? 0 : userItemCartDeleteRank.get(userId));
            features[260] = String.valueOf(null == userItemBuyRank.get(userId) ? 0 : userItemBuyRank.get(userId));
            features[261] = String.valueOf(null == userItemFollowRank.get(userId) ? 0 : userItemFollowRank.get(userId));

            features[262] = String.valueOf(null == itemUserClick.get(skuId) ? 0 : itemUserClick.get(skuId));
            features[263] = String.valueOf(null == itemUserDetail.get(skuId) ? 0 : itemUserDetail.get(skuId));
            features[264] = String.valueOf(null == itemUserCart.get(skuId) ? 0 : itemUserCart.get(skuId));
            features[265] = String.valueOf(null == itemUserCartDelete.get(skuId) ? 0 : itemUserCartDelete.get(skuId));
            features[266] = String.valueOf(null == itemUserBuy.get(skuId) ? 0 : itemUserBuy.get(skuId));
            features[267] = String.valueOf(null == itemUserFollow.get(skuId) ? 0 : itemUserFollow.get(skuId));

            features[268] = String.valueOf(null == itemUserClickRank.get(skuId) ? 0 : itemUserClickRank.get(skuId));
            features[269] = String.valueOf(null == itemUserDetailRank.get(skuId) ? 0 : itemUserDetailRank.get(skuId));
            features[270] = String.valueOf(null == itemUserCartRank.get(skuId) ? 0 : itemUserCartRank.get(skuId));
            features[271] = String.valueOf(null == itemUserCartDeleteRank.get(skuId) ? 0 : itemUserCartDeleteRank.get(skuId));
            features[272] = String.valueOf(null == itemUserBuyRank.get(skuId) ? 0 : itemUserBuyRank.get(skuId));
            features[273] = String.valueOf(null == itemUserFollowRank.get(skuId) ? 0 : itemUserFollowRank.get(skuId));

            features[274] = String.valueOf(null == userRatioUserClickCross.get(userId) ? 0 : userRatioUserClickCross.get(userId));
            features[275] = String.valueOf(null == userRatioUserDetailCross.get(userId) ? 0 : userRatioUserDetailCross.get(userId));
            features[276] = String.valueOf(null == userRatioUserCartCross.get(userId) ? 0 : userRatioUserCartCross.get(userId));
            features[277] = String.valueOf(null == userRatioUserCartDeleteCross.get(userId) ? 0 : userRatioUserCartDeleteCross.get(userId));
            features[278] = String.valueOf(null == userRatioUserFollowCross.get(userId) ? 0 : userRatioUserFollowCross.get(userId));

            features[279] = String.valueOf(null == userRatioUserItemClickCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemClickCross.get(userId + "_" + skuId));
            features[280] = String.valueOf(null == userRatioUserItemDetailCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemDetailCross.get(userId + "_" + skuId));
            features[281] = String.valueOf(null == userRatioUserItemCartCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemCartCross.get(userId + "_" + skuId));
            features[282] = String.valueOf(null == userRatioUserItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemCartDeleteCross.get(userId + "_" + skuId));
            features[283] = String.valueOf(null == userRatioUserItemFollowCross.get(userId + "_" + skuId) ? 0 : userRatioUserItemFollowCross.get(userId + "_" + skuId));

            features[284] = String.valueOf(null == itemRatioItemClickCross.get(skuId) ? 0 : itemRatioItemClickCross.get(skuId));
            features[285] = String.valueOf(null == itemRatioItemDetailCross.get(skuId) ? 0 : itemRatioItemDetailCross.get(skuId));
            features[286] = String.valueOf(null == itemRatioItemCartCross.get(skuId) ? 0 : itemRatioItemCartCross.get(skuId));
            features[287] = String.valueOf(null == itemRatioItemCartDeleteCross.get(skuId) ? 0 : itemRatioItemCartDeleteCross.get(skuId));
            features[288] = String.valueOf(null == itemRatioItemFollowCross.get(skuId) ? 0 : itemRatioItemFollowCross.get(skuId));

            features[289] = String.valueOf(null == itemRatioUserItemClickCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemClickCross.get(userId + "_" + skuId));
            features[290] = String.valueOf(null == itemRatioUserItemDetailCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemDetailCross.get(userId + "_" + skuId));
            features[291] = String.valueOf(null == itemRatioUserItemCartCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemCartCross.get(userId + "_" + skuId));
            features[292] = String.valueOf(null == itemRatioUserItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemCartDeleteCross.get(userId + "_" + skuId));
            features[293] = String.valueOf(null == itemRatioUserItemFollowCross.get(userId + "_" + skuId) ? 0 : itemRatioUserItemFollowCross.get(userId + "_" + skuId));

            features[294] = String.valueOf(null == userItemRatioUserClickCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserClickCross.get(userId + "_" + skuId));
            features[295] = String.valueOf(null == userItemRatioUserDetailCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserDetailCross.get(userId + "_" + skuId));
            features[296] = String.valueOf(null == userItemRatioUserCartCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserCartCross.get(userId + "_" + skuId));
            features[297] = String.valueOf(null == userItemRatioUserCartDeleteCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserCartDeleteCross.get(userId + "_" + skuId));
            features[298] = String.valueOf(null == userItemRatioUserFollowCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserFollowCross.get(userId + "_" + skuId));
            features[299] = String.valueOf(null == userItemRatioItemClickCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemClickCross.get(userId + "_" + skuId));
            features[300] = String.valueOf(null == userItemRatioItemDetailCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemDetailCross.get(userId + "_" + skuId));
            features[301] = String.valueOf(null == userItemRatioItemCartCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemCartCross.get(userId + "_" + skuId));
            features[302] = String.valueOf(null == userItemRatioItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemCartDeleteCross.get(userId + "_" + skuId));
            features[303] = String.valueOf(null == userItemRatioItemFollowCross.get(userId + "_" + skuId) ? 0 : userItemRatioItemFollowCross.get(userId + "_" + skuId));
            features[304] = String.valueOf(null == userItemRatioUserItemClickCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemClickCross.get(userId + "_" + skuId));
            features[305] = String.valueOf(null == userItemRatioUserItemDetailCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemDetailCross.get(userId + "_" + skuId));
            features[306] = String.valueOf(null == userItemRatioUserItemCartCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemCartCross.get(userId + "_" + skuId));
            features[307] = String.valueOf(null == userItemRatioUserItemCartDeleteCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemCartDeleteCross.get(userId + "_" + skuId));
            features[308] = String.valueOf(null == userItemRatioUserItemFollowCross.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemFollowCross.get(userId + "_" + skuId));

            features[309] = String.valueOf(null == userRatioUserClickCrossRank.get(userId) ? 0 : userRatioUserClickCrossRank.get(userId));
            features[310] = String.valueOf(null == userRatioUserDetailCrossRank.get(userId) ? 0 : userRatioUserDetailCrossRank.get(userId));
            features[311] = String.valueOf(null == userRatioUserCartCrossRank.get(userId) ? 0 : userRatioUserCartCrossRank.get(userId));
            features[312] = String.valueOf(null == userRatioUserCartDeleteCrossRank.get(userId) ? 0 : userRatioUserCartDeleteCrossRank.get(userId));
            features[313] = String.valueOf(null == userRatioUserFollowCrossRank.get(userId) ? 0 : userRatioUserFollowCrossRank.get(userId));

            features[314] = String.valueOf(null == userRatioUserItemClickCrossRank.get(userId + "_" + skuId) ? 0 : userRatioUserItemClickCrossRank.get(userId + "_" + skuId));
            features[315] = String.valueOf(null == userRatioUserItemDetailCrossRank.get(userId + "_" + skuId) ? 0 : userRatioUserItemDetailCrossRank.get(userId + "_" + skuId));
            features[316] = String.valueOf(null == userRatioUserItemCartCrossRank.get(userId + "_" + skuId) ? 0 : userRatioUserItemCartCrossRank.get(userId + "_" + skuId));
            features[317] = String.valueOf(null == userRatioUserItemCartDeleteCrossRank.get(userId + "_" + skuId) ? 0 : userRatioUserItemCartDeleteCrossRank.get(userId + "_" + skuId));
            features[318] = String.valueOf(null == userRatioUserItemFollowCrossRank.get(userId + "_" + skuId) ? 0 : userRatioUserItemFollowCrossRank.get(userId + "_" + skuId));

            features[319] = String.valueOf(null == itemRatioItemClickCrossRank.get(skuId) ? 0 : itemRatioItemClickCrossRank.get(skuId));
            features[320] = String.valueOf(null == itemRatioItemDetailCrossRank.get(skuId) ? 0 : itemRatioItemDetailCrossRank.get(skuId));
            features[321] = String.valueOf(null == itemRatioItemCartCrossRank.get(skuId) ? 0 : itemRatioItemCartCrossRank.get(skuId));
            features[322] = String.valueOf(null == itemRatioItemCartDeleteCrossRank.get(skuId) ? 0 : itemRatioItemCartDeleteCrossRank.get(skuId));
            features[323] = String.valueOf(null == itemRatioItemFollowCrossRank.get(skuId) ? 0 : itemRatioItemFollowCrossRank.get(skuId));

            features[324] = String.valueOf(null == itemRatioUserItemClickCrossRank.get(userId + "_" + skuId) ? 0 : itemRatioUserItemClickCrossRank.get(userId + "_" + skuId));
            features[325] = String.valueOf(null == itemRatioUserItemDetailCrossRank.get(userId + "_" + skuId) ? 0 : itemRatioUserItemDetailCrossRank.get(userId + "_" + skuId));
            features[326] = String.valueOf(null == itemRatioUserItemCartCrossRank.get(userId + "_" + skuId) ? 0 : itemRatioUserItemCartCrossRank.get(userId + "_" + skuId));
            features[327] = String.valueOf(null == itemRatioUserItemCartDeleteCrossRank.get(userId + "_" + skuId) ? 0 : itemRatioUserItemCartDeleteCrossRank.get(userId + "_" + skuId));
            features[328] = String.valueOf(null == itemRatioUserItemFollowCrossRank.get(userId + "_" + skuId) ? 0 : itemRatioUserItemFollowCrossRank.get(userId + "_" + skuId));

            features[329] = String.valueOf(null == userItemRatioUserClickCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserClickCrossRank.get(userId + "_" + skuId));
            features[330] = String.valueOf(null == userItemRatioUserDetailCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserDetailCrossRank.get(userId + "_" + skuId));
            features[331] = String.valueOf(null == userItemRatioUserCartCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserCartCrossRank.get(userId + "_" + skuId));
            features[332] = String.valueOf(null == userItemRatioUserCartDeleteCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserCartDeleteCrossRank.get(userId + "_" + skuId));
            features[333] = String.valueOf(null == userItemRatioUserFollowCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserFollowCrossRank.get(userId + "_" + skuId));
            features[334] = String.valueOf(null == userItemRatioItemClickCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioItemClickCrossRank.get(userId + "_" + skuId));
            features[335] = String.valueOf(null == userItemRatioItemDetailCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioItemDetailCrossRank.get(userId + "_" + skuId));
            features[336] = String.valueOf(null == userItemRatioItemCartCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioItemCartCrossRank.get(userId + "_" + skuId));
            features[337] = String.valueOf(null == userItemRatioItemCartDeleteCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioItemCartDeleteCrossRank.get(userId + "_" + skuId));
            features[338] = String.valueOf(null == userItemRatioItemFollowCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioItemFollowCrossRank.get(userId + "_" + skuId));
            features[339] = String.valueOf(null == userItemRatioUserItemClickCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemClickCrossRank.get(userId + "_" + skuId));
            features[340] = String.valueOf(null == userItemRatioUserItemDetailCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemDetailCrossRank.get(userId + "_" + skuId));
            features[341] = String.valueOf(null == userItemRatioUserItemCartCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemCartCrossRank.get(userId + "_" + skuId));
            features[342] = String.valueOf(null == userItemRatioUserItemCartDeleteCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemCartDeleteCrossRank.get(userId + "_" + skuId));
            features[343] = String.valueOf(null == userItemRatioUserItemFollowCrossRank.get(userId + "_" + skuId) ? 0 : userItemRatioUserItemFollowCrossRank.get(userId + "_" + skuId));


            if(!isPredict){
                Long count = labelMap.get(userId + "_" + skuId);
                if(null == count){
                    features[344] = String.valueOf(1);
                    ++negative;
                }
                else {
                    features[344] = String.valueOf(0);
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

























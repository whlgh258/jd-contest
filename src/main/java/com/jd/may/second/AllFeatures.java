package com.jd.may.second;

import au.com.bytecode.opencsv.CSVWriter;
import com.alibaba.fastjson.JSONObject;
import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.util.*;

/**
 * Created by wanghl on 17-5-7.
 */
public class AllFeatures {
    private static final Logger log = Logger.getLogger(AllFeatures.class);
    private static final String path = "/home/wanghl/jd_contest/0507/";
    private static final int[] modelIds = new int[]{11,217,27,216,17,210,14,211,218,26,28,24,220,21,25,15,221,222,29,23,19,16,223,219,116,224,22,31,125,13,111,18,33,119,333,112,322,311,318,110,319,34,12,124,120,121,325,331,329,334,346,328,32,320,341,348,340,323,337,343,345,336,312,321,335,347,344,342,316,315,36,115,114,113,122,317,212,313,35,314,39,338,225,310,324,332,327,37,38,330};
    private static final Map<String, Integer> attributes;
    private static final Map<String, Map<String, Object>> userInfos;

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
        attributes.put("click", i++);
        attributes.put("detail", i++);
        attributes.put("cart", i++);
        attributes.put("cart_delete", i++);
        attributes.put("buy", i++);
        attributes.put("follow", i++);
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


        attributes.put("target", i++);

        userInfos = new HashMap<>();
        String userInfoSql = "select user_id,sku_id,age,sex,user_level,attr1,attr2,attr3,cate,brand,comment_num,has_bad_comment,bad_comment_rate from user_action_1";
        List<Map<String, Object>> result = DBOperation.queryBySql(userInfoSql);
        log.info("user info size: " + result.size());
        for(Map<String, Object> row : result){
            String userId = String.valueOf(row.get("user_id"));
            userInfos.put(userId, row);
        }
    }

    public static List<String[]> features(String start, String end, String labelStart, String labelEnd, boolean isPredict) throws Exception{
        List<String[]> lines = new ArrayList<>();

        String userCountSql = "select user_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String userSumSql = "select user_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String userAvgSql = "select user_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(buy)+1),3) as buy,round(log(avg(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String userBuyRatioSql = "select user_id,round(if(sum(click)>0,sum(buy)/sum(click),0),3) as click,round(if(sum(detail)>0,sum(buy)/sum(detail),0),3) as detail,round(if(sum(cart)>0,sum(buy)/sum(cart),0),3) as cart,round (if(sum(cart_delete)>0,sum(buy)/sum(cart_delete),0),3) as cart_delete,round(if(sum(buy>0),sum(buy)/sum(buy),0),3) as buy,round(if(sum(follow)>0,sum(buy)/sum(follow),0),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
        String itemCountSql = "select sku_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String itemSumSql = "select sku_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String itemAvgSql = "select sku_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(buy)+1),3) as buy,round(log(avg(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String itemBuyRatioSql = "select sku_id,round(if(sum(click)>0,sum(buy)/sum(click),0),3) as click,round(if(sum(detail)>0,sum(buy)/sum(detail),0),3) as detail,round(if(sum(cart)>0,sum(buy)/sum(cart),0),3) as cart,round (if(sum(cart_delete)>0,sum(buy)/sum(cart_delete),0),3) as cart_delete,round(if(sum(buy>0),sum(buy)/sum(buy),0),3) as buy,round(if(sum(follow)>0,sum(buy)/sum(follow),0),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        String userItemCountSql = "select user_id,sku_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        String userItemSumSql = "select user_id,sku_id,round(log(sum(click)+1),3) as click,round(log(sum(detail>0)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        String userItemAvgSql = "select user_id,sku_id,round(log(avg(click)+1),3) as click,round(log(avg(detail>0)+1),3) as detail,round(log(avg(cart)+1),3) as cart,round(log(avg(cart_delete)+1),3) as cart_delete,round(log(avg(buy)+1),3) as buy,round(log(avg(follow)+1),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";
        String userItemBuyRatioSql = "select user_id,sku_id,round(if(sum(click)>0,sum(buy)/sum(click),0),3) as click,round(if(sum(detail)>0,sum(buy)/sum(detail),0),3) as detail,round(if(sum(cart)>0,sum(buy)/sum(cart),0),3) as cart,round (if(sum(cart_delete)>0,sum(buy)/sum(cart_delete),0),3) as cart_delete,round(if(sum(buy>0),sum(buy)/sum(buy),0),3) as buy,round(if(sum(follow)>0,sum(buy)/sum(follow),0),3) as follow from user_action_1 where action_date>='" + start + "' and action_date<='" + end + "' group by user_id,sku_id";

        Map<String, Map<String, Double>> userCountFeature = Features.countFeature(userCountSql, 0, false);
        Map<String, Map<String, Double>> userSumFeature = Features.sumFeature(userSumSql, 0, false);
        Map<String, Map<String, Double>> userAvgFeature = Features.avgFeature(userAvgSql, 0, false);
        Map<String, Map<String, Double>> userBuyRatioFeature = Features.buyRatioFeature(userBuyRatioSql, 0, true);
        Map<String, Map<String, Double>> itemCountFeature = Features.countFeature(itemCountSql, 1, false);
        Map<String, Map<String, Double>> itemSumFeature = Features.sumFeature(itemSumSql, 1, false);
        Map<String, Map<String, Double>> itemAvgFeature = Features.avgFeature(itemAvgSql, 1, false);
        Map<String, Map<String, Double>> itemBuyRatioFeature = Features.buyRatioFeature(itemBuyRatioSql, 1, true);
        Map<String, Map<String, Double>> userItemCountFeature = Features.countFeature(userItemCountSql, 2, false);
        Map<String, Map<String, Double>> userItemSumFeature = Features.sumFeature(userItemSumSql, 2, false);
        Map<String, Map<String, Double>> userItemAvgFeature = Features.avgFeature(userItemAvgSql, 2, false);
        Map<String, Map<String, Double>> userItemBuyRatioFeature = Features.buyRatioFeature(userItemBuyRatioSql, 2, true);

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
        String userItemFollowSql = "select user_id,round(log(count(distinct sku_id)+1),3) as count from user_action_1 where click>0 and action_date>='" + start + "' and action_date<='" + end + "' group by user_id";
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
        String itemUserFollowSql = "select sku_id,round(log(count(distinct user_id)+1),3) as count from user_action_1 where click>0 and action_date>='" + start + "' and action_date<='" + end + "' group by sku_id";
        Map<String, Double> itemUserFollow = ItemBuyUsers.itemUserCount(itemUserFollowSql, "sku_id");
        Map<String, Integer> itemUserClickRank = Rank.rank(itemUserClick);
        Map<String, Integer> itemUserDetailRank = Rank.rank(itemUserDetail);
        Map<String, Integer> itemUserCartRank = Rank.rank(itemUserCart);
        Map<String, Integer> itemUserCartDeleteRank = Rank.rank(itemUserCartDelete);
        Map<String, Integer> itemUserBuyRank = Rank.rank(itemUserBuy);
        Map<String, Integer> itemUserFollowRank = Rank.rank(itemUserFollow);

        Map<String, Integer> labelMap = new HashMap<>();
        if(!isPredict){
            String labelSql = "select user_id,sku_id,count(1) as count from user_action_1 where buy>0 and action_date>='" + labelStart + "' and action_date<='" + labelEnd + "'";
            log.info("label sql: " + labelSql);
            List<Map<String, Object>> labelResult = DBOperation.queryBySql(labelSql);
            for(Map<String, Object> row : labelResult){
                String userId = String.valueOf(row.get("user_id"));
                String skuId = String.valueOf(row.get("sku_id"));
                int count = (int) row.get("count");

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

            Map<String, Object> userInfo = userInfos.get(userId);

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

            setFeature(features, userCountFeature.get(userId), 109, false);
            setFeature(features, userSumFeature.get(userId), 115, false);
            setFeature(features, userAvgFeature.get(userId), 121, false);
            setFeature(features, userBuyRatioFeature.get(userId), 127, false);

            setFeature(features, itemCountFeature.get(skuId), 132, false);
            setFeature(features, itemSumFeature.get(skuId), 138, false);
            setFeature(features, itemAvgFeature.get(skuId), 144, false);
            setFeature(features, itemBuyRatioFeature.get(skuId), 150, false);

            setFeature(features, userItemCountFeature.get(userId + "_" + skuId), 155, false);
            setFeature(features, userItemSumFeature.get(userId + "_" + skuId), 161, false);
            setFeature(features, userItemAvgFeature.get(userId + "_" + skuId), 167, false);
            setFeature(features, userItemBuyRatioFeature.get(userId + "_" + skuId), 173, false);

            features[178] = String.valueOf(userClickCountRank.get(userId));
            features[179] = String.valueOf(userDetailCountRank.get(userId));
            features[180] = String.valueOf(userCartCountRank.get(userId));
            features[181] = String.valueOf(userCartDeleteCountRank.get(userId));
            features[182] = String.valueOf(userBuyCountRank.get(userId));
            features[183] = String.valueOf(userFollowCountRank.get(userId));
            features[184] = String.valueOf(userClickSumRank.get(userId));
            features[185] = String.valueOf(userDetailSumRank.get(userId));
            features[186] = String.valueOf(userCartSumRank.get(userId));
            features[187] = String.valueOf(userCartDeleteSumRank.get(userId));
            features[188] = String.valueOf(userBuySumRank.get(userId));
            features[189] = String.valueOf(userFollowSumRank.get(userId));
            features[190] = String.valueOf(userClickAvgRank.get(userId));
            features[191] = String.valueOf(userDetailAvgRank.get(userId));
            features[192] = String.valueOf(userCartAvgRank.get(userId));
            features[193] = String.valueOf(userCartDeleteAvgRank.get(userId));
            features[194] = String.valueOf(userBuyAvgRank.get(userId));
            features[195] = String.valueOf(userFollowAvgRank.get(userId));
            features[196] = String.valueOf(userBuyClickRatioRank.get(userId));
            features[197] = String.valueOf(userBuyDetailRatioRank.get(userId));
            features[198] = String.valueOf(userBuyCartRatioRank.get(userId));
            features[199] = String.valueOf(userBuyCartDeleteRatioRank.get(userId));
            features[200] = String.valueOf(userBuyFollowRatioRank.get(userId));
            features[201] = String.valueOf(itemClickCountRank.get(skuId));
            features[202] = String.valueOf(itemDetailCountRank.get(skuId));
            features[203] = String.valueOf(itemCartCountRank.get(skuId));
            features[204] = String.valueOf(itemCartDeleteCountRank.get(skuId));
            features[205] = String.valueOf(itemBuyCountRank.get(skuId));
            features[206] = String.valueOf(itemFollowCountRank.get(skuId));
            features[207] = String.valueOf(itemClickSumRank.get(skuId));
            features[208] = String.valueOf(itemDetailSumRank.get(skuId));
            features[209] = String.valueOf(itemCartSumRank.get(skuId));
            features[210] = String.valueOf(itemCartDeleteSumRank.get(skuId));
            features[211] = String.valueOf(itemBuySumRank.get(skuId));
            features[212] = String.valueOf(itemFollowSumRank.get(skuId));
            features[213] = String.valueOf(itemClickAvgRank.get(skuId));
            features[214] = String.valueOf(itemDetailAvgRank.get(skuId));
            features[215] = String.valueOf(itemCartAvgRank.get(skuId));
            features[216] = String.valueOf(itemCartDeleteAvgRank.get(skuId));
            features[217] = String.valueOf(itemBuyAvgRank.get(skuId));
            features[218] = String.valueOf(itemFollowAvgRank.get(skuId));
            features[219] = String.valueOf(itemBuyClickRatioRank.get(skuId));
            features[220] = String.valueOf(itemBuyDetailRatioRank.get(skuId));
            features[221] = String.valueOf(itemBuyCartRatioRank.get(skuId));
            features[222] = String.valueOf(itemBuyCartDeleteRatioRank.get(skuId));
            features[223] = String.valueOf(itemBuyFollowRatioRank.get(skuId));
            features[224] = String.valueOf(userItemClickCountRank.get(userId + "_" + skuId));
            features[225] = String.valueOf(userItemDetailCountRank.get(userId + "_" + skuId));
            features[226] = String.valueOf(userItemCartCountRank.get(userId + "_" + skuId));
            features[227] = String.valueOf(userItemCartDeleteCountRank.get(userId + "_" + skuId));
            features[228] = String.valueOf(userItemBuyCountRank.get(userId + "_" + skuId));
            features[229] = String.valueOf(userItemFollowCountRank.get(userId + "_" + skuId));
            features[230] = String.valueOf(userItemClickSumRank.get(userId + "_" + skuId));
            features[231] = String.valueOf(userItemDetailSumRank.get(userId + "_" + skuId));
            features[232] = String.valueOf(userItemCartSumRank.get(userId + "_" + skuId));
            features[233] = String.valueOf(userItemCartDeleteSumRank.get(userId + "_" + skuId));
            features[234] = String.valueOf(userItemBuySumRank.get(userId + "_" + skuId));
            features[235] = String.valueOf(userItemFollowSumRank.get(userId + "_" + skuId));
            features[236] = String.valueOf(userItemClickAvgRank.get(userId + "_" + skuId));
            features[237] = String.valueOf(userItemDetailAvgRank.get(userId + "_" + skuId));
            features[238] = String.valueOf(userItemCartAvgRank.get(userId + "_" + skuId));
            features[239] = String.valueOf(userItemCartDeleteAvgRank.get(userId + "_" + skuId));
            features[240] = String.valueOf(userItemBuyAvgRank.get(userId + "_" + skuId));
            features[241] = String.valueOf(userItemFollowAvgRank.get(userId + "_" + skuId));
            features[242] = String.valueOf(userItemBuyClickRatioRank.get(userId + "_" + skuId));
            features[243] = String.valueOf(userItemBuyDetailRatioRank.get(userId + "_" + skuId));
            features[244] = String.valueOf(userItemBuyCartRatioRank.get(userId + "_" + skuId));
            features[245] = String.valueOf(userItemBuyCartDeleteRatioRank.get(userId + "_" + skuId));
            features[246] = String.valueOf(userItemBuyFollowRatioRank.get(userId + "_" + skuId));

            features[247] = String.valueOf(userPopular.get(userId));
            features[248] = String.valueOf(userPopularRank.get(userId));
            features[249] = String.valueOf(itemPopular.get(skuId));
            features[250] = String.valueOf(itemPopularRank.get(skuId));
            features[251] = String.valueOf(itemActionUserCount.get(skuId));
            features[252] = String.valueOf(itemActionUserRank.get(skuId));

            features[253] = String.valueOf(userIsBuy.get(userId));
            features[254] = String.valueOf(itemIsBuy.get(skuId));
            features[255] = String.valueOf(itemBuyDate.get(skuId));

            features[256] = String.valueOf(userItemClick.get(userId));
            features[257] = String.valueOf(userItemDetail.get(userId));
            features[258] = String.valueOf(userItemCart.get(userId));
            features[259] = String.valueOf(userItemCartDelete.get(userId));
            features[260] = String.valueOf(userItemBuy.get(userId));
            features[261] = String.valueOf(userItemFollow.get(userId));

            features[262] = String.valueOf(userItemClickRank.get(userId));
            features[263] = String.valueOf(userItemDetailRank.get(userId));
            features[264] = String.valueOf(userItemCartRank.get(userId));
            features[265] = String.valueOf(userItemCartDeleteRank.get(userId));
            features[266] = String.valueOf(userItemBuyRank.get(userId));
            features[267] = String.valueOf(userItemFollowRank.get(userId));

            features[268] = String.valueOf(itemUserClick.get(skuId));
            features[269] = String.valueOf(itemUserDetail.get(skuId));
            features[270] = String.valueOf(itemUserCart.get(skuId));
            features[271] = String.valueOf(itemUserCartDelete.get(skuId));
            features[272] = String.valueOf(itemUserBuy.get(skuId));
            features[273] = String.valueOf(itemUserFollow.get(skuId));

            features[274] = String.valueOf(itemUserClickRank.get(skuId));
            features[275] = String.valueOf(itemUserDetailRank.get(skuId));
            features[276] = String.valueOf(itemUserCartRank.get(skuId));
            features[277] = String.valueOf(itemUserCartDeleteRank.get(skuId));
            features[278] = String.valueOf(itemUserBuyRank.get(skuId));
            features[279] = String.valueOf(itemUserFollowRank.get(skuId));

            if(!isPredict){
                Integer count = labelMap.get(userId + "_" + skuId);
                if(null == count){
                    features[280] = String.valueOf(1);
                    ++negative;
                }
                else {
                    features[280] = String.valueOf(0);
                    ++positive;
                }
            }

            lines.add(features);
        }

        log.info("positive: " + positive);
        log.info("negative: " + negative);

        CSVWriter testWriter = new CSVWriter(new FileWriter(path + "test.csv"), ',', '\0');
        testWriter.writeNext(attributes.keySet().toArray(new String[0]));
        testWriter.writeAll(lines);
        testWriter.close();

        return lines;
    }

    private static void setFeature(String[] features, Map<String, Double> map, int index, boolean isRatio){
        if(isRatio){
            features[index + 1] = String.valueOf(map.get("click"));
            features[index + 2] = String.valueOf(map.get("detail"));
            features[index + 3] = String.valueOf(map.get("cart"));
            features[index + 4] = String.valueOf(map.get("cartDelete"));
            features[index + 5] = String.valueOf(map.get("follow"));
        }
        else {
            features[index + 1] = String.valueOf(map.get("click"));
            features[index + 2] = String.valueOf(map.get("detail"));
            features[index + 3] = String.valueOf(map.get("cart"));
            features[index + 4] = String.valueOf(map.get("cartDelete"));
            features[index + 5] = String.valueOf(map.get("buy"));
            features[index + 6] = String.valueOf(map.get("follow"));
        }
    }
}

























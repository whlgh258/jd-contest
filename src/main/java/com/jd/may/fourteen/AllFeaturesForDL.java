package com.jd.may.fourteen;

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

        attributes.put("user_popular", i++);
        attributes.put("item_popular", i++);

        attributes.put("item_action_user", i++);

        attributes.put("user_distinct_item_click", i++);
        attributes.put("user_distinct_item_detail", i++);
        attributes.put("user_distinct_item_cart", i++);
        attributes.put("user_distinct_item_cart_delete", i++);
        attributes.put("user_distinct_item_follow", i++);

        attributes.put("item_distinct_user_click", i++);
        attributes.put("item_distinct_user_detail", i++);
        attributes.put("item_distinct_user_cart", i++);
        attributes.put("item_distinct_user_cart_delete", i++);
        attributes.put("item_distinct_user_follow", i++);

        attributes.put("target", i++);
        log.info("feature size: " + attributes.size());
    }

    public static List<String[]> features(String filename, String tablename, boolean isPredict) throws Exception{
        List<String[]> lines = new ArrayList<>();

        Map<String,Map<String, Object>> userInfos = new HashMap<>();
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

        Map<String, Map<String, Double>> userCountFeature = Features.countFeature(userCountSql, 0);
        Map<String, Map<String, Double>> userSumFeature = Features.sumFeature(userSumSql, 0);
        Map<String, Map<String, Double>> userAvgFeature = Features.avgFeature(userAvgSql, 0);
        Map<String, Map<String, Double>> itemCountFeature = Features.countFeature(itemCountSql, 1);
        Map<String, Map<String, Double>> itemSumFeature = Features.sumFeature(itemSumSql, 1);
        Map<String, Map<String, Double>> itemAvgFeature = Features.avgFeature(itemAvgSql, 1);
        Map<String, Map<String, Double>> userItemCountFeature = Features.countFeature(userItemCountSql, 2);
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

                for(Map.Entry<String, Integer> entry : modelIdMap.entrySet()){
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


            features[148] = String.valueOf(null == userPopular.get(userId) ? 0 : userPopular.get(userId));
            features[149] = String.valueOf(null == itemPopular.get(skuId) ? 0 : itemPopular.get(skuId));
            features[150] = String.valueOf(null == itemActionUserCount.get(skuId) ? 0 : itemActionUserCount.get(skuId));

            features[151] = String.valueOf(null == userItemClick.get(userId) ? 0 : userItemClick.get(userId));
            features[152] = String.valueOf(null == userItemDetail.get(userId) ? 0 : userItemDetail.get(userId));
            features[153] = String.valueOf(null == userItemCart.get(userId) ? 0 : userItemCart.get(userId));
            features[154] = String.valueOf(null == userItemCartDelete.get(userId) ? 0 : userItemCartDelete.get(userId));
            features[155] = String.valueOf(null == userItemFollow.get(userId) ? 0 : userItemFollow.get(userId));

            features[156] = String.valueOf(null == itemUserClick.get(skuId) ? 0 : itemUserClick.get(skuId));
            features[157] = String.valueOf(null == itemUserDetail.get(skuId) ? 0 : itemUserDetail.get(skuId));
            features[158] = String.valueOf(null == itemUserCart.get(skuId) ? 0 : itemUserCart.get(skuId));
            features[159] = String.valueOf(null == itemUserCartDelete.get(skuId) ? 0 : itemUserCartDelete.get(skuId));
            features[160] = String.valueOf(null == itemUserFollow.get(skuId) ? 0 : itemUserFollow.get(skuId));

            if(!isPredict){
                Long count = labelMap.get(userId + "_" + skuId);
                if(null == count){
                    features[161] = String.valueOf(1);
                    ++negative;
                }
                else {
                    features[161] = String.valueOf(0);
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

    private static void setFeature(String[] features, Map<String, Double> map, int index){
        features[index] = String.valueOf(null == map.get("click") ? 0 : map.get("click"));
        features[index + 1] = String.valueOf(null == map.get("detail") ? 0 : map.get("detail"));
        features[index + 2] = String.valueOf(null == map.get("cart") ? 0 : map.get("cart"));
        features[index + 3] = String.valueOf(null == map.get("cartDelete") ? 0 : map.get("cartDelete"));
        features[index + 4] = String.valueOf(null == map.get("follow") ? 0 : map.get("follow"));
    }
}

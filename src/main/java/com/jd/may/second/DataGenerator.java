package com.jd.may.second;

import au.com.bytecode.opencsv.CSVWriter;
import com.alibaba.fastjson.JSONObject;
import multithreads.DBOperation;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileWriter;
import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;

/**
 * Created by wanghl on 17-4-30.
 */
public class DataGenerator {
    private static final Logger log = Logger.getLogger(DataGenerator.class);

    private static final int[] windows = new int[]{5/*1, 2, 3, 4, 5, 7, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55, 60, 65*/};
    private static final LocalDate last = LocalDate.parse("2016-04-15");
    private static final LocalDate first = LocalDate.parse("2016-02-01");

    public static void main(String[] args) throws Exception {
        int slide = 5;
        for(int window : windows){
            if(window < 5){
                slide = window;
            }
            else {
                slide = 5;
            }

//            File dir = new File(path + window);
//            if(!dir.exists()){
//                dir.mkdirs();
//            }

            /**
             * https://tianchi.aliyun.com/competition/new_articleDetail.html?spm=5176.8366600.0.0.uhCB1n&raceId=&postsId=99
             * select user_id,round(log(count(if(click>0,1,null))+1),3) as click,round(log(count(if(detail>0,1,null))+1),3) as detail,round(log(count(if(cart>0,1,null))+1),3) as cart,round(log(count(if(cart_delete>0,1,null))+1),3) as cart_delete,round(log(count(if(buy>0,1,null))+1),3) as buy,round(log(count(if(follow>0,1,null))+1),3) as follow from user_action_1 group by user_id order by click desc limit 10;
             * select user_id,round(log(sum(click)+1),3) as click,round(log(sum(detail)+1),3) as detail,round(log(sum(cart)+1),3) as cart,round(log(sum(cart_delete)+1),3) as cart_delete,round(log(sum(buy)+1),3) as buy,round(log(sum(follow)+1),3) as follow from user_action_1 group by user_id order by click desc limit 10;
             * 一、特征
             * 1、user
             * 2、item
             * 3、user-item：交互时间
             * 二、维度
             * 1、计数
             * select user_id,count(if(click>0,1,null)) as click,count(if(detail>0,1,null)) as detail,count(if(cart>0,1,null)) as cart,count(if(cart_delete>0,1,null)) as cart_delete,count(if(buy>0,1,null)) as buy,count(if(follow>0,1,null)) as follow from user_action_1 where action_date>='' and action_date<='' group by user_id
             * select sku_id,count(if(click>0,1,null)) as click,count(if(detail>0,1,null)) as detail,count(if(cart>0,1,null)) as cart,count(if(cart_delete>0,1,null)) as cart_delete,count(if(buy>0,1,null)) as buy,count(if(follow>0,1,null)) as follow from user_action_1 where action_date>='' and action_date<='' group by sku_id
             * select user_id,sku_id,count(if(click>0,1,null)) as click,count(if(detail>0,1,null)) as detail,count(if(cart>0,1,null)) as cart,count(if(cart_delete>0,1,null)) as cart_delete,count(if(buy>0,1,null)) as buy,count(if(follow>0,1,null)) as follow from user_action_1 where action_date>='' and action_date<='' group by user_id,sku_id
             * 2、求和
             * select user_id,sum(click) as click,sum(detail) as detail,sum(cart) as cart,sum(cart_delete) as cart_delete,sum(buy) as buy,sum(follow) as follow from user_action_1 where action_date>='' and action_date<='' group by user_id
             * select sku_id,sum(click) as click,sum(detail) as detail,sum(cart) as cart,sum(cart_delete) as cart_delete,sum(buy) as buy,sum(follow) as follow from user_action_1 where action_date>='' and action_date<='' group by sku_id
             * select user_id,sku_id,sum(click) as click,sum(detail) as detail,sum(cart) as cart,sum(cart_delete) as cart_delete,sum(buy) as buy,sum(follow) as follow from user_action_1 where action_date>='' and action_date<='' group by user_id,sku_id
             * 3、比值
             * select user_id,count(if(buy>0,1,null))/count(if(click>0,1,null)) as buy_click_ratio,count(if(buy>0,1,null))/count(if(detail>0,1,null)) as buy_detail_ratio,count(if(buy>0,1,null))/count(if(cart>0,1,null)) as buy_cart_ratio,count(if(buy>0,1,null))/count(if(cart_delete,1,null)) as buy_cart_delete_ratio,count(if(buy>0,1,null))/count(if(follow,1,null)) as buy_follow_ratio from user_action_1 where action_date>='' and action_date<='' group by user_id
             * select sku_id,count(if(buy>0,1,null))/count(if(click>0,1,null)) as buy_click_ratio,count(if(buy>0,1,null))/count(if(detail>0,1,null)) as buy_detail_ratio,count(if(buy>0,1,null))/count(if(cart>0,1,null)) as buy_cart_ratio,count(if(buy>0,1,null))/count(if(cart_delete,1,null)) as buy_cart_delete_ratio,count(if(buy>0,1,null))/count(if(follow,1,null)) as buy_follow_ratio from user_action_1 where action_date>='' and action_date<='' group by sku_id
             * select user_id,sku_id,count(if(buy>0,1,null))/count(if(click>0,1,null)) as buy_click_ratio,count(if(buy>0,1,null))/count(if(detail>0,1,null)) as buy_detail_ratio,count(if(buy>0,1,null))/count(if(cart>0,1,null)) as buy_cart_ratio,count(if(buy>0,1,null))/count(if(cart_delete,1,null)) as buy_cart_delete_ratio,count(if(buy>0,1,null))/count(if(follow,1,null)) as buy_follow_ratio from user_action_1 where action_date>='' and action_date<='' group by user_id,sku_id
             *
             * select user_id,sum(buy)/sum(click) as buy_click_ratio,sum(buy)/sum(detail) as buy_detail_ratio,sum(buy)/sum(cart) as buy_cart_ratio,sum(buy)/sum(cart_delete) as buy_cart_delete_ratio,sum(buy)/sum(follow) as buy_follow_ratio from user_action_1 where action_date>='' and action_date<='' group by user_id
             * select sku_id,sum(buy)/sum(click) as buy_click_ratio,sum(buy)/sum(detail) as buy_detail_ratio,sum(buy)/sum(cart) as buy_cart_ratio,sum(buy)/sum(cart_delete) as buy_cart_delete_ratio,sum(buy)/sum(follow) as buy_follow_ratio from user_action_1 where action_date>='' and action_date<='' group by sku_id
             * select user_id,sku_id,sum(buy)/sum(click) as buy_click_ratio,sum(buy)/sum(detail) as buy_detail_ratio,sum(buy)/sum(cart) as buy_cart_ratio,sum(buy)/sum(cart_delete) as buy_cart_delete_ratio,sum(buy)/sum(follow) as buy_follow_ratio from user_action_1 where action_date>='' and action_date<='' group by user_id,sku_id
             * 4、排名
             * Map<Double, String>、行为次数、购买人数、5种转化率、人均行为
             * 5、user：活跃度，连续购买天数
             * 6、item：商品热度、交互时间、交互人数，连续购买天数、是否重复购买
             * 7、平均值
             * 8、action/sum(action)
             * 9、购买用户数/浏览用户数、购买用户数/点击用户数、、、
             * 10、UI行为数*I转化率、UI行为数*U购买比例、UI行为/U行为、
             * 三、交叉
             * 1、user × user-item
             * 2、item × user-item
             * 四、
             * 交互区间内是否购买
             * 交互区间内购买天数
             */

            log.info("window = " + window + ", slide = " + slide);
            LocalDate predictEndDate = last;
            LocalDate predictStartDate = predictEndDate.minusDays(window - 1);
            LocalDate testLabelEndDate = last;
            LocalDate testLabelStartDate = testLabelEndDate.minusDays(slide - 1);
            LocalDate testEndDate = testLabelEndDate.minusDays(slide);
            LocalDate testStartDate = testEndDate.minusDays(window - 1);
            LocalDate validLabelEndDate = testLabelEndDate.minusDays(slide);
            LocalDate validLabelStartDate = validLabelEndDate.minusDays(slide - 1);
            LocalDate validEndDate = testEndDate.minusDays(slide);
            LocalDate validStartDate = validEndDate.minusDays(window - 1);

            String predictStart = predictStartDate.toString();
            String predictEnd = predictEndDate.toString();
            String testLabelEnd = testLabelEndDate.toString();
            String testLabelStart = testLabelStartDate.toString();
            String testEnd = testEndDate.toString();
            String testStart = testStartDate.toString();
            String validLabelEnd = validLabelEndDate.toString();
            String validLabelStart = validLabelStartDate.toString();
            String validEnd = validEndDate.toString();
            String validStart = validStartDate.toString();


            log.info("predict: " + predictStart + " - " + predictEnd);
            log.info("test   : " + testStart + " - " + testEnd + " *** " + testLabelStart + " - " + testLabelEnd);
            log.info("valid  : " + validStart + " - " + validEnd + " *** " + validLabelStart + " - " + validLabelEnd);

            List<String[]> lines = AllFeatures.features(testStart, testEnd, testLabelStart, testLabelEnd, false);

            for(LocalDate date = validLabelEndDate; date.minusDays((window - 1) + 2 * slide).isAfter(first.minusDays(1)); date = date.minusDays(slide)){
                LocalDate trainLabelEndDate = date.minusDays(slide);
                LocalDate trainLabelStartDate = trainLabelEndDate.minusDays(slide - 1);
                LocalDate trainEndDate = trainLabelEndDate.minusDays(slide);
                LocalDate trainStartDate = trainEndDate.minusDays(window - 1);

                String trainLabelEnd = trainLabelEndDate.toString();
                String trainLabelStart = trainLabelStartDate.toString();
                String trainEnd = trainEndDate.toString();
                String trainStart = trainStartDate.toString();

                log.info("train  : " + trainStart + " - " + trainEnd + " *** " + trainLabelStart + " - " + trainLabelEnd);

            }

        }
        String startDate = "2016-04-15";
        String endDate = "2016-02-01";
        LocalDate startTime = LocalDate.parse(startDate);
        LocalDate endTime = LocalDate.parse(endDate);
        /*for(LocalDate start = startTime; start.isBefore(endTime.plusDays(1L)); start = start.plusDays(window)){
            LocalDate localPredictDate = start;
            LocalDate localTestLabelDate = start;
            LocalDate localTestTrainDate = start.minusDays(slide);
//            LocalDate localValidLabelDate = ;
//            LocalDate lcoalValidTrainDate = ;
//            LocalDate localTrainLabelDate = ;
//            LocalDate localTrainTrainDate = ;


        }
        List<String> dates = getDates(startDate, endDate);
        for(String date : dates){
            LocalDate ld = LocalDate.parse(date);
            String trainDate = ld.minusDays(4L).toString();
            String trainLabelDate = ld.minusDays(3L).toString();
            String validDate = trainLabelDate;
            String validLabelDate = ld.minusDays(2L).toString();
            String testDate = validLabelDate;
            String testLabelDate = ld.minusDays(1L).toString();
            String predictTrainDate = testLabelDate;
            String predictDate = date;

            log.info("train date: " + trainDate);
            log.info("train label date: " + trainLabelDate);
            log.info("valid date: " + validDate);
            log.info("valid label date: " + validLabelDate);
            log.info("test date: " + testDate);
            log.info("test label date: " + testLabelDate);
            log.info("predict date: " + predictTrainDate);
            log.info("predict label date: " + predictDate);

            int[] modelIds = new int[]{11,217,27,216,17,210,14,211,218,26,28,24,220,21,25,15,221,222,29,23,19,16,223,219,116,224,22,31,125,13,111,18,33,119,333,112,322,311,318,110,319,34,12,124,120,121,325,331,329,334,346,328,32,320,341,348,340,323,337,343,345,336,312,321,335,347,344,342,316,315,36,115,114,113,122,317,212,313,35,314,39,338,225,310,324,332,327,37,38,330};
//            String[] header = new String[]{"user_id", "sku_id", "age", "sex", "user_level", "attr1", "attr2", "attr3", "cate", "brand", "comment_num", "has_bad_comment", "bad_comment_rate", "click", "detail", "cart", "cart_delete", "buy", "follow", "target"};
            *//*List<String> attributes = new ArrayList<>();
            attributes.add("user_id");
            attributes.add("sku_id");
            attributes.add("age");
            attributes.add("sex");
            attributes.add("user_level");
            attributes.add("attr1");
            attributes.add("attr2");
            attributes.add("attr3");
            attributes.add("cate");
            attributes.add("brand");
            attributes.add("comment_num");
            attributes.add("has_bad_comment");
            attributes.add("bad_comment_rate");
            attributes.add("click");
            attributes.add("detail");
            attributes.add("cart");
            attributes.add("cart_delete");
            attributes.add("buy");
            attributes.add("follow");
            for(int modelId : modelIds){
                attributes.add("model_" + modelId);
            }

            attributes.add("user_total_click");
            attributes.add("user_total_detail");
            attributes.add("user_total_cart");
            attributes.add("user_total_cart_delete");
            attributes.add("user_total_buy");
            attributes.add("user_total_follow");
            attributes.add("user_buy_click_ratio");
            attributes.add("user_buy_detail_ratio");
            attributes.add("user_buy_cart_ratio");
            attributes.add("user_buy_cart_delete_ratio");
            attributes.add("user_buy_follow_ratio");
            attributes.add("product_popular");
            attributes.add("brand_popular");

            attributes.add("target");*//*

            Map<String, Integer> attributes = new LinkedHashMap<>();
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

            attributes.put("user_total_click", i++);
            attributes.put("user_total_detail", i++);
            attributes.put("user_total_cart", i++);
            attributes.put("user_total_cart_delete", i++);
            attributes.put("user_total_buy", i++);
            attributes.put("user_total_follow", i++);
            attributes.put("user_buy_click_ratio", i++);
            attributes.put("user_buy_detail_ratio", i++);
            attributes.put("user_buy_cart_ratio", i++);
            attributes.put("user_buy_cart_delete_ratio", i++);
            attributes.put("user_buy_follow_ratio", i++);
            attributes.put("product_total_click", i++);
            attributes.put("product_total_detail", i++);
            attributes.put("product_total_cart", i++);
            attributes.put("product_total_cart_delete", i++);
            attributes.put("product_total_buy", i++);
            attributes.put("product_total_follow", i++);
            attributes.put("product_buy_click_ratio", i++);
            attributes.put("product_buy_detail_ratio", i++);
            attributes.put("product_buy_cart_ratio", i++);
            attributes.put("product_buy_cart_delete_ratio", i++);
            attributes.put("product_buy_follow_ratio", i++);
            attributes.put("brand_total_click", i++);
            attributes.put("brand_total_detail", i++);
            attributes.put("brand_total_cart", i++);
            attributes.put("brand_total_cart_delete", i++);
            attributes.put("brand_total_buy", i++);
            attributes.put("brand_total_follow", i++);
            attributes.put("brand_buy_click_ratio", i++);
            attributes.put("brand_buy_detail_ratio", i++);
            attributes.put("brand_buy_cart_ratio", i++);
            attributes.put("brand_buy_cart_delete_ratio", i++);
            attributes.put("brand_buy_follow_ratio", i++);
            attributes.put("product_popular", i++);
            attributes.put("brand_popular", i++);

            attributes.put("target", i++);

            List<String[]> trainLines = new ArrayList<>();
            Map<Integer, List<Object>> trainUserInfoMap = userInfo(trainDate);
            Map<Integer, List<Object>> trainProductInfoMap = productInfo(trainDate);
            Map<Integer, List<Object>> trainBrandInfoMap = brandInfo(trainDate);
            Map<Integer, Double> trainProductPopularMap = productPopular(trainDate);
            Map<Integer, Double> trainBrandPopularMap = brandPopular(trainDate);
            CSVWriter trainWriter = new CSVWriter(new FileWriter(path + "train.csv"), ',', '\0');
            String trainSql = "select user_id,sku_id,age,sex,user_level,attr1,attr2,attr3,cate,brand,comment_num,has_bad_comment,bad_comment_rate,model_id,click,detail,cart,cart_delete,buy,follow from user_action_1 where action_date='" + trainDate + "'";
            log.info("train sql: " + trainSql);
            List<Map<String, Object>> trainResult = DBOperation.queryBySql(trainSql);
            log.info("train size: " + trainResult.size());
            Map<String, Integer> trainMap = new HashedMap();
            trainMap.put("positive", 0);
            trainMap.put("negative", 0);
            for(Map<String, Object> row : trainResult){
                int userId = (int) row.get("user_id");
                int skuId= (int) row.get("sku_id");
                String[] features = work(row, attributes, trainUserInfoMap, trainProductInfoMap, trainBrandInfoMap, trainProductPopularMap, trainBrandPopularMap, false);

                String trainLabelSql = "select buy from user_action_1 where user_id=" + userId + " and sku_id=" + skuId + " and action_date='" + trainLabelDate + "'";
                List<Map<String, Object>> trainLabelResult = DBOperation.queryBySql(trainLabelSql);

                setTarget(trainLabelResult, features, trainMap);
                trainLines.add(features);
            }

            log.info("train positive: " + trainMap.get("positive"));
            log.info("train negative: " + trainMap.get("negative"));

            trainWriter.writeNext(attributes.keySet().toArray(new String[0]));
            trainWriter.writeAll(trainLines);
            trainWriter.close();

            List<String[]> validLines = new ArrayList<>();
            Map<Integer, List<Object>> validUserInfoMap = userInfo(validDate);
            Map<Integer, List<Object>> validProductInfoMap = productInfo(validDate);
            Map<Integer, List<Object>> validBrandInfoMap = brandInfo(validDate);
            Map<Integer, Double> validProductPopularMap = productPopular(validDate);
            Map<Integer, Double> validBrandPopularMap = brandPopular(validDate);
            CSVWriter validWriter = new CSVWriter(new FileWriter(path + "valid.csv"), ',', '\0');
            String validSql = "select user_id,sku_id,age,sex,user_level,attr1,attr2,attr3,cate,brand,comment_num,has_bad_comment,bad_comment_rate,model_id,click,detail,cart,cart_delete,buy,follow from user_action_1 where action_date='" + validDate + "'";
            List<Map<String, Object>> validResult = DBOperation.queryBySql(validSql);
            log.info("valid sql: " + validSql);
            log.info("valid size: " + validResult.size());
            Map<String, Integer> validMap = new HashedMap();
            validMap.put("positive", 0);
            validMap.put("negative", 0);
            for(Map<String, Object> row : validResult){
                int userId = (int) row.get("user_id");
                int skuId= (int) row.get("sku_id");
                String[] features = work(row, attributes, validUserInfoMap, validProductInfoMap, validBrandInfoMap, validProductPopularMap, validBrandPopularMap, false);

                String validLabelSql = "select buy from user_action_1 where user_id=" + userId + " and sku_id=" + skuId + " and action_date='" + validLabelDate + "'";
                List<Map<String, Object>> validLabelResult = DBOperation.queryBySql(validLabelSql);
                setTarget(validLabelResult, features, validMap);

                validLines.add(features);
            }

            log.info("valid positive: " + validMap.get("positive"));
            log.info("valid negative: " + validMap.get("negative"));

            validWriter.writeNext(attributes.keySet().toArray(new String[0]));
            validWriter.writeAll(validLines);
            validWriter.close();

            List<String[]> testLines = new ArrayList<>();
            Map<Integer, List<Object>> testUserInfoMap = userInfo(testDate);
            Map<Integer, List<Object>> testProductInfoMap = productInfo(testDate);
            Map<Integer, List<Object>> testBrandInfoMap = brandInfo(testDate);
            Map<Integer, Double> testProductPopularMap = productPopular(testDate);
            Map<Integer, Double> testBrandPopularMap = brandPopular(testDate);
            CSVWriter testWriter = new CSVWriter(new FileWriter(path + "test.csv"), ',', '\0');
            String testSql = "select user_id,sku_id,age,sex,user_level,attr1,attr2,attr3,cate,brand,comment_num,has_bad_comment,bad_comment_rate,model_id,click,detail,cart,cart_delete,buy,follow from user_action_1 where action_date='" + testDate + "'";
            List<Map<String, Object>> testResult = DBOperation.queryBySql(testSql);
            log.info("test sql: " + testSql);
            log.info("test size: " + testResult.size());
            Map<String, Integer> testMap = new HashMap<>();
            testMap.put("positive", 0);
            testMap.put("negative", 0);
            for(Map<String, Object> row : testResult){
                int userId = (int) row.get("user_id");
                int skuId= (int) row.get("sku_id");
                String[] features = work(row, attributes, testUserInfoMap, testProductInfoMap, testBrandInfoMap, testProductPopularMap, testBrandPopularMap, false);


                String testLabelSql = "select buy from user_action_1 where user_id=" + userId + " and sku_id=" + skuId + " and action_date='" + testLabelDate + "'";
                List<Map<String, Object>> testLabelResult = DBOperation.queryBySql(testLabelSql);
                setTarget(testLabelResult, features, testMap);

                testLines.add(features);
            }

            log.info("test positive: " + testMap.get("positive"));
            log.info("test negative: " + testMap.get("negative"));

            testWriter.writeNext(attributes.keySet().toArray(new String[0]));
            testWriter.writeAll(testLines);
            testWriter.close();

            List<String[]> predictLines = new ArrayList<>();
            Map<Integer, List<Object>> predictUserInfoMap = userInfo(predictTrainDate);
            Map<Integer, List<Object>> predictProductInfoMap = productInfo(predictTrainDate);
            Map<Integer, List<Object>> predictBrandInfoMap = brandInfo(predictTrainDate);
            Map<Integer, Double> predictProductPopularMap = productPopular(predictTrainDate);
            Map<Integer, Double> predictBrandPopularMap = brandPopular(predictTrainDate);
            CSVWriter predictWriter = new CSVWriter(new FileWriter(path + "predict.csv"), ',', '\0');
            String predictSql = "select user_id,sku_id,age,sex,user_level,attr1,attr2,attr3,cate,brand,comment_num,has_bad_comment,bad_comment_rate,model_id,click,detail,cart,cart_delete,buy,follow from user_action_1 where action_date='" + predictTrainDate + "'";
            List<Map<String, Object>> predictResult = DBOperation.queryBySql(predictSql);
            log.info("predict sql: " + predictSql);
            log.info("predict size: " + predictResult.size());
            Map<String, Integer> predictMap = new HashMap<>();
            predictMap.put("positive", 0);
            predictMap.put("negative", 0);
            for(Map<String, Object> row : predictResult){
                int userId = (int) row.get("user_id");
                int skuId= (int) row.get("sku_id");
                String[] features = work(row, attributes, predictUserInfoMap, predictProductInfoMap, predictBrandInfoMap, predictProductPopularMap, predictBrandPopularMap, false);

                String predictLabelSql = "select buy from user_action_1 where user_id=" + userId + " and sku_id=" + skuId + " and action_date='" + predictDate + "'";
                List<Map<String, Object>> predictLabelResult = DBOperation.queryBySql(predictLabelSql);
                setTarget(predictLabelResult, features, predictMap);

                predictLines.add(features);
            }

            log.info("predict positive: " + predictMap.get("positive"));
            log.info("predict negative: " + predictMap.get("negative"));

            predictWriter.writeNext(attributes.keySet().toArray(new String[0]));
            predictWriter.writeAll(predictLines);
            predictWriter.close();

            List<String[]> finalLines = new ArrayList<>();
            Map<Integer, List<Object>> finalUserInfoMap = userInfo(predictDate);
            Map<Integer, List<Object>> finalProductInfoMap = productInfo(predictDate);
            Map<Integer, List<Object>> finalBrandInfoMap = brandInfo(predictDate);
            Map<Integer, Double> finalProductPopularMap = productPopular(predictDate);
            Map<Integer, Double> finalBrandPopularMap = brandPopular(predictDate);
            CSVWriter finalWriter = new CSVWriter(new FileWriter(path + "final.csv"), ',', '\0');
            String finalSql = "select user_id,sku_id,age,sex,user_level,attr1,attr2,attr3,cate,brand,comment_num,has_bad_comment,bad_comment_rate,model_id,click,detail,cart,cart_delete,buy,follow from user_action_1 where action_date='" + predictDate + "'";
            List<Map<String, Object>> finalResult = DBOperation.queryBySql(finalSql);
            log.info("final sql: " + finalSql);
            log.info("final size: " + finalResult.size());
            for(Map<String, Object> row : finalResult){
                String[] features = work(row, attributes, finalUserInfoMap, finalProductInfoMap, finalBrandInfoMap, finalProductPopularMap, finalBrandPopularMap, true);
                finalLines.add(features);
            }

            List<String> list = new ArrayList<>(attributes.keySet());
            List<String> header = list.subList(0, attributes.size() - 1);
            finalWriter.writeNext(header.toArray(new String[0]));
            finalWriter.writeAll(finalLines);
            finalWriter.close();
        }*/
    }

    private static List<String> getDates(String startDate, String endDate){
        List<String> dates = new ArrayList<>();
        LocalDate start = LocalDate.parse(startDate);
        LocalDate end = LocalDate.parse(endDate);

        for(LocalDate date = end; date.isAfter(start.minusDays(1L)); date = date.minusDays(1L)){
            dates.add(date.toString());
        }

        return dates;
    }

    private static String[] work(Map<String, Object> row, Map<String, Integer> attributes, Map<Integer, List<Object>> userInfoMap, Map<Integer, List<Object>> productInfoMap, Map<Integer, List<Object>> brandInfoMap, Map<Integer, Double> productPopularMap, Map<Integer, Double> brandPopularMap, boolean isFinal){
        int length = 0;
        if(isFinal){
            length = attributes.size() - 1;
        }
        else{
            length = attributes.size();
        }

        String[] features = new String[length];
        for(int i = 0; i < length; i++){
            features[i] = "0";
        }

        int userId = (int) row.get("user_id");
        int skuId= (int) row.get("sku_id");
        int age = (int) row.get("age");
        int sex = (int) row.get("sex");
        int userLevel = (int) row.get("user_level");
        int attr1 = (int) row.get("attr1");
        int attr2 = (int) row.get("attr2");
        int attr3 = (int) row.get("attr3");
        int cate = (int) row.get("cate");
        int brand = (int) row.get("brand");
        int commentNum = (int) row.get("comment_num");
        int hasBadComment = (int) row.get("has_bad_comment");
        double badCommentRate = (double) row.get("bad_comment_rate");
        int click = (int) row.get("click");
        int detail = (int) row.get("detail");
        int cart = (int) row.get("cart");
        int cartDelete = (int) row.get("cart_delete");
        int buy = (int) row.get("buy");
        int follow = (int) row.get("follow");
        String modelIdStr = (String) row.get("model_id");

        features[0] = String.valueOf(userId);
        features[1] = String.valueOf(skuId);
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
        features[13] = String.valueOf(click);
        features[14] = String.valueOf(detail);
        features[15] = String.valueOf(cart);
        features[16] = String.valueOf(cartDelete);
        features[17] = String.valueOf(buy);
        features[18] = String.valueOf(follow);

        if(StringUtils.isNotBlank(modelIdStr) && !"null".equalsIgnoreCase(modelIdStr)){
            JSONObject json = JSONObject.parseObject(modelIdStr);
            for(String key : json.keySet()){
                int index = attributes.get("model_" + key);
                features[index] = String.valueOf(json.getIntValue(key));
            }
        }

        List<Object> userInfo = userInfoMap.get(userId);
        List<Object> productInfo = productInfoMap.get(skuId);
        List<Object> brandInfo = brandInfoMap.get(brand);

//        double productPopular = 0.0;
//        if(null != productPopularMap.get(skuId)){
//            productPopular = productPopularMap.get(skuId);
//        }

        double productPopular = productPopularMap.get(skuId);
        double brandPopular = brandPopularMap.get(brand);
        features[109] = String.valueOf(userInfo.get(0));
        features[110] = String.valueOf(userInfo.get(1));
        features[111] = String.valueOf(userInfo.get(2));
        features[112] = String.valueOf(userInfo.get(3));
        features[113] = String.valueOf(userInfo.get(4));
        features[114] = String.valueOf(userInfo.get(5));
        features[115] = String.valueOf(userInfo.get(6));
        features[116] = String.valueOf(userInfo.get(7));
        features[117] = String.valueOf(userInfo.get(8));
        features[118] = String.valueOf(userInfo.get(9));
        features[119] = String.valueOf(userInfo.get(10));
        features[120] = String.valueOf(productInfo.get(0));
        features[121] = String.valueOf(productInfo.get(1));
        features[122] = String.valueOf(productInfo.get(2));
        features[123] = String.valueOf(productInfo.get(3));
        features[124] = String.valueOf(productInfo.get(4));
        features[125] = String.valueOf(productInfo.get(5));
        features[126] = String.valueOf(productInfo.get(6));
        features[127] = String.valueOf(productInfo.get(7));
        features[128] = String.valueOf(productInfo.get(8));
        features[129] = String.valueOf(productInfo.get(9));
        features[130] = String.valueOf(productInfo.get(10));
        features[131] = String.valueOf(brandInfo.get(0));
        features[132] = String.valueOf(brandInfo.get(1));
        features[133] = String.valueOf(brandInfo.get(2));
        features[134] = String.valueOf(brandInfo.get(3));
        features[135] = String.valueOf(brandInfo.get(4));
        features[136] = String.valueOf(brandInfo.get(5));
        features[137] = String.valueOf(brandInfo.get(6));
        features[138] = String.valueOf(brandInfo.get(7));
        features[139] = String.valueOf(brandInfo.get(8));
        features[140] = String.valueOf(brandInfo.get(9));
        features[141] = String.valueOf(brandInfo.get(10));
        features[142] = String.valueOf(productPopular);
        features[143] = String.valueOf(brandPopular);

        return features;
    }

    private static void setTarget(List<Map<String, Object>> result, String[] features, Map<String, Integer> map){
        if(result.size() > 0){
            int target = (int) result.get(0).get("buy");
            if(target > 0){
                features[144] = String.valueOf(0);
                map.put("positive", map.get("positive") + 1);
            }
            else {
                features[144] = String.valueOf(1);
                map.put("negative", map.get("negative") + 1);
            }
        }
        else {
            features[144] = String.valueOf(1);
            map.put("negative", map.get("negative") + 1);
        }
    }

    private static Map<Integer, List<Object>> userInfo(String date){
        String sql = "select user_id,sum(click) as sum_click,sum(detail) as sum_detail,sum(cart) as sum_cart,sum(cart_delete) as sum_cart_delete," +
                "sum(buy) as sum_buy,sum(follow) as sum_follow,sum(buy)/sum(click) as buy_click_ratio,sum(buy)/sum(detail) as buy_detail_ratio," +
                "sum(buy)/sum(cart) as buy_cart_ratio,sum(buy)/sum(cart_delete) as buy_cart_delete_ratio,sum(buy)/sum(follow) as buy_follow_ratio " +
                "from user_action_1 where action_date in('" + date + "') group by user_id";
        log.info("user info sql: " + sql);

        return info(sql, "user_id");
    }

    private static Map<Integer, List<Object>> productInfo(String date){
        String sql = "select sku_id,sum(click) as sum_click,sum(detail) as sum_detail,sum(cart) as sum_cart,sum(cart_delete) as sum_cart_delete," +
                "sum(buy) as sum_buy,sum(follow) as sum_follow,sum(buy)/sum(click) as buy_click_ratio,sum(buy)/sum(detail) as buy_detail_ratio," +
                "sum(buy)/sum(cart) as buy_cart_ratio,sum(buy)/sum(cart_delete) as buy_cart_delete_ratio,sum(buy)/sum(follow) as buy_follow_ratio " +
                "from user_action_1 where action_date in('" + date + "') group by sku_id";
        log.info("product info sql: " + sql);

        return info(sql, "sku_id");
    }

    private static Map<Integer, List<Object>> brandInfo(String date) {
        String sql = "select brand,sum(click) as sum_click,sum(detail) as sum_detail,sum(cart) as sum_cart,sum(cart_delete) as sum_cart_delete," +
                "sum(buy) as sum_buy,sum(follow) as sum_follow,sum(buy)/sum(click) as buy_click_ratio,sum(buy)/sum(detail) as buy_detail_ratio," +
                "sum(buy)/sum(cart) as buy_cart_ratio,sum(buy)/sum(cart_delete) as buy_cart_delete_ratio,sum(buy)/sum(follow) as buy_follow_ratio " +
                "from user_action_1 where action_date in('" + date + "') group by brand";
        log.info("brand info sql: " + sql);

        return info(sql, "brand");
    }

    private static Map<Integer, List<Object>> info(String sql, String key){
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        Map<Integer, List<Object>> map = new HashMap<>();
        for(Map<String, Object> row : result){
            int id = (int) row.get(key);
            int sumClick = ((BigDecimal)row.get("sum_click")).intValue();
            int sumDetail = ((BigDecimal)row.get("sum_detail")).intValue();
            int sumCart = ((BigDecimal)row.get("sum_cart")).intValue();
            int sumCartDelete = ((BigDecimal)row.get("sum_cart_delete")).intValue();
            int sumBuy = ((BigDecimal)row.get("sum_buy")).intValue();
            int sumFollow = ((BigDecimal)row.get("sum_follow")).intValue();

            double buyClickRatio = 0.0;
            if(null != row.get("buy_click_ratio")){
                buyClickRatio = ((BigDecimal)row.get("buy_click_ratio")).doubleValue();
            }

            double buyDetailRatio = 0.0;
            if(null != row.get("buy_detail_ratio")){
                buyDetailRatio = ((BigDecimal)row.get("buy_detail_ratio")).doubleValue();
            }

            double buyCartRatio = 0.0;
            if(null != row.get("buy_cart_ratio")){
                buyCartRatio = ((BigDecimal)row.get("buy_cart_ratio")).doubleValue();
            }

            double buyCartDeleteRatio = 0.0;
            if(null != row.get("buy_cart_delete_ratio")){
                buyCartDeleteRatio = ((BigDecimal)row.get("buy_cart_delete_ratio")).doubleValue();
            }

            double buyFollowRatio = 0.0;
            if(null != row.get("buy_follow_ratio")){
                buyFollowRatio = ((BigDecimal)row.get("buy_follow_ratio")).doubleValue();
            }

            List<Object> list = new ArrayList<>();
            list.add(sumClick);
            list.add(sumDetail);
            list.add(sumCart);
            list.add(sumCartDelete);
            list.add(sumBuy);
            list.add(sumFollow);
            list.add(buyClickRatio);
            list.add(buyDetailRatio);
            list.add(buyCartRatio);
            list.add(buyCartDeleteRatio);
            list.add(buyFollowRatio);

            map.put(id, list);
        }

        return map;
    }
}


























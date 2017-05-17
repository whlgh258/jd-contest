package com.jd.may.fifteen;

import java.io.File;

import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-4-30.
 */
public class DataGenerator {
    private static final Logger log = Logger.getLogger(DataGenerator.class);
    //https://github.com/h2oai/h2o-3/blob/angela-docs/h2o-docs/src/product/tutorials/GridSearch.md

    public static void main(String[] args) throws Exception {
        /**
         * https://tianchi.aliyun.com/competition/new_articleDetail.html?spm=5176.8366600.0.0.uhCB1n&raceId=&postsId=99
         *
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
         * 用户click购买率×总户总点击、用户click购买率×用户对某个item的点击，其余类似，增加10个特征
         * 商品click购买率×商品总点击、商品click购买率×商品被某个user的点击，其余类似，增加10个特征
         * 四、
         * 交互区间内是否购买
         * 交互区间内购买天数
         *
         * 五、
         * 1、训练数据集
         * 从购买出发，往前推，直到action_period为1，特征值需要按购买的action_period和购买之前的action_period的差值进行衰减，以体现出来不同的action时间对于购买的影响
         * 2、预测数据集
         * 以要预测区间的action_period为基准0，往前逐步衰减各个区间的action的各项指标
         * 3、不同的购买区间生成不同的训练数据，否则购买数据和动作数据会在时间区间上重合，影响训练
         *
         * select count(concat_ws('_',user_id,sku_id)) from user_action_1 where action_date>='2016-04-11' and action_date<='2016-04-15' and concat_ws('_',user_id,sku_id) in (select concat_ws('_',user_id,sku_id) from user_action_1 where buy>0 and action_date>='2016-04-11' and action_date<='2016-04-15');
         */

//        AllFeaturesForGBM.features("predict_GBM.csv", "user_action_3", true);
//        AllFeaturesForGBM.features("data_GBM.csv", "user_action_2", false);
//        AllFeaturesForDL.features("predict_DL.csv", "user_action_3", true);
//        AllFeaturesForDL.features("data_DL.csv", "user_action_2", false);


        for(int i = 1; i < 15; i++){
            String path = "/home/wanghl/jd_contest/0517/";
            path += i + "/";
            File file = new File(path);
            if(!file.exists()){
                file.mkdirs();
            }
            for(int j = 1; j < 15 && (i + j) <= 15; j++){
                System.out.println(i + ": " + j + ": " + (j + 1) + "--->" + (j + i));
                String filename = "data_DL_" + j + "_" + (j + 1) + "_" + (j + i) + ".csv";
                AllFeaturesForDL.features(filename, j, (j + 1), (j + i), "user_action_2", false, file.getAbsolutePath());
                log.info("--------------------------------------------------------------------------------------------------------------------------");
            }
        }

        /*for(int j = 1; j <= 15; j++){
            System.out.println(0 + ": " + j + ": " + (1) + "--->" + (j));
            String path = "/home/wanghl/jd_contest/0517/";
            path += j + "/";
            File file = new File(path);
            if(!file.exists()){
                file.mkdirs();
            }
            String filename = "predict_DL_" + 0 + "_" + 1 + "_" + j + ".csv";
            System.out.println(filename);
            AllFeaturesForDL.features(filename, -1, 1, j, "user_action_3", true, file.getAbsolutePath());
            System.out.println("--------------------------------------------------------------------------------------------------------------------------");
        }*/
    }
}


























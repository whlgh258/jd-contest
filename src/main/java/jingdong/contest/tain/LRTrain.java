package jingdong.contest.tain;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegression$;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wanghl on 17-4-16.
 */
public class LRTrain {
    private static final Logger log = Logger.getLogger(LRTrain.class);
    private static final String filepath = "hdfs://big-data1:9000/jd/0416/";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("jd-contest").setMaster("spark://big-data1:7077").setJars(new String[]{"/home/wanghl/IdeaProjects/jd-contest/target/jd-contest-1.0.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("spark://big-data1:7077").appName("jd-contest").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();

        Dataset<Row> allData = session.read().option("header","true").option("inferSchema","true").csv(filepath + "train.csv");
        System.out.println("data has " + allData.count() + " rows");
        System.out.println(allData.columns().length);

        VectorAssembler assembler = new VectorAssembler();
        List<String> features = new ArrayList<>();
        features.add("age");
        features.add("sex");
        features.add("user_level");
        features.add("attr1");
        features.add("attr2");
        features.add("attr3");
        features.add("cate");
        features.add("brand");
        features.add("comment_num");
        features.add("has_bad_comment");
        for(int i = 40; i <= 75; i++){
            features.add("click_" + i);
            features.add("detail_" + i);
            features.add("cart_" + i);
            features.add("cart_delete_" + i);
            features.add("buy_" + i);
            features.add("follow_" + i);
        }

        int[] modelIds = new int[]{11,217,27,216,17,210,14,211,218,26,28,24,220,21,25,15,221,222,29,23,19,16,223,219,116,224,22,31,125,13,111,18,33,119,333,112,322,311,318,110,319,34,12,124,120,121,325,331,329,334,346,328,32,320,341,348,340,323,337,343,345,336,312,321,335,347,344,342,316,315,36,115,114,113,122,317,212,313,35,314,39,338,225,310,324,332,327,37,38,330};
        for(int modelId : modelIds){
            features.add("model_" + modelId);
        }

        assembler.setInputCols(features.toArray(new String[0]));
        assembler.setOutputCol("features");
        Dataset<Row> data = assembler.transform(allData);

        Dataset<Row>[] datasets = data.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> train = datasets[0];
        Dataset<Row> test = datasets[1];
//        System.out.println("Train = " + train.count()+" Test = " + test.count());
        train.cache();

        LogisticRegression lor = new LogisticRegression().setFeaturesCol("features").setLabelCol("buy").setRegParam(0.0).setElasticNetParam(0.0).setMaxIter(100).setTol(1E-6).setFitIntercept(true);
        LogisticRegressionModel model = lor.fit(train);

        Dataset<Row> predictions = model.transform(test);
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();
        evaluator.setLabelCol("buy");
        double accuracy = evaluator.evaluate(predictions);
        System.out.println("Test accuracy = " + accuracy);
        System.out.println(predictions.count());

        predictions.show(5);

        Dataset<Row> result = predictions.select("user_id", "sku_id", "buy", "prediction");
        System.out.println(result.count());
        List<Row> rows = result.takeAsList((int) result.count());
        System.out.println(rows.size());

        Set<Integer> realAndPredBuyUser = new HashSet<>();    // tp
        Set<Integer> realAndPredNotBuyUser = new HashSet<>(); // fn
        Set<Integer> notAndPredBuyUser = new HashSet<>();     // fp
        Set<Integer> notAndPredNotBuyuUser = new HashSet<>(); // tn

//        Set<String> realAndPredBuyUserProdcut = new HashSet<>();    // tp
//        Set<String> realAndPredNotBuyUserProduct = new HashSet<>(); // fn
//        Set<String> notAndPredBuyUserProduct = new HashSet<>();     // fp
//        Set<String> notAndPredNotBuyUserProduct = new HashSet<>();  // tn

        Set<String> realBuyUserProduct = new HashSet<>();
        Set<String> realNotBuyUserProduct = new HashSet<>();

        Set<String> predBuyUserProduct = new HashSet<>();
        Set<String> predNotBuyUserProduct = new HashSet<>();

        int tp = 0;
        int fn = 0;
        int fp = 0;
        int tn = 0;
        for(Row row : rows){
            int userId = row.getAs("user_id");
            int skuId = row.getAs("sku_id");
            int buy = row.getAs("buy");
            double prediction1 = row.getAs("prediction");
            int prediction = (int)prediction1;
//            System.out.println(buy + ": " + prediction);

            String key = userId + "_" + skuId;

            if(1 == buy && 1 == prediction){
                ++tp;
                realAndPredBuyUser.add(userId);
            }
            else if(1 == buy && 0 == prediction){
                ++fn;
                realAndPredNotBuyUser.add(userId);
            }
            else if(0 == buy && 1 == prediction){
                ++fp;
                notAndPredBuyUser.add(userId);
            }
            else {
                ++tn;
                notAndPredNotBuyuUser.add(userId);
            }

            if(1 == buy){
                realBuyUserProduct.add(key);
            }

            if(0 == buy){
                realNotBuyUserProduct.add(key);
            }

            if(1 == prediction){
                predBuyUserProduct.add(key);
            }

            if(0 == prediction){
                predNotBuyUserProduct.add(key);
            }
        }

        System.out.println("tp = " + tp + ", fn = " + fn + ", fp = " + fp + ", tn = " + tn);
        double precision = tp * 1.0 / (tp + fp);
        double recall = tp * 1.0 / (tp + fn);

        System.out.println("precision = " + precision + ", recall = " + recall);

        double f = (2 * precision * recall) / (precision + recall);
        System.out.println("f = " + f);

        // f11
        int userTp = realAndPredBuyUser.size();
        int userFn = realAndPredNotBuyUser.size();
        int userFp = notAndPredBuyUser.size();
        int userTn = notAndPredNotBuyuUser.size();
        System.out.println("userTp = " + userTp + ", userFn = " + userFn + ", userFp = " + userFp + ", userTn = " + userTn);
        double userPrecision = userTp * 1.0 / (userTp + userFp);
        double userRecall = userTp * 1.0 / (userTp + userFn);
        System.out.println("userPrecision = " + userPrecision + ", userRecall = " + userRecall);
        double f11 = (6 * userPrecision * userRecall) / (userPrecision + 5 * userRecall);
        System.out.println("f11 = " + f11);

        System.out.println("realBuyUserProduct: " + realBuyUserProduct.size() + ", realNotBuyUserProduct: " + realNotBuyUserProduct.size() +
                           ", predBuyUserProduct: " + predBuyUserProduct.size() + ", predNotBuyUserProduct: " + predNotBuyUserProduct.size());
        // f12
        // tp
        Set<String> realBuyUserProductTmp = new HashSet<>(realBuyUserProduct);
        Set<String> predBuyUserProductTmp = new HashSet<>(predBuyUserProduct);
        realBuyUserProductTmp.retainAll(predBuyUserProductTmp);
        int userProductTp = realBuyUserProductTmp.size();

        // fn
        realBuyUserProductTmp = new HashSet<>(realBuyUserProduct);
        Set<String> predNotBuyUserProductTmp = new HashSet<>(predNotBuyUserProduct);
        realBuyUserProductTmp.retainAll(predNotBuyUserProductTmp);
        int userProductFn = realBuyUserProductTmp.size();

        // fp
        Set<String> realNotBuyUserProductTmp = new HashSet<>(realNotBuyUserProduct);
        realNotBuyUserProductTmp.retainAll(predBuyUserProductTmp);
        int userProductFp = realNotBuyUserProductTmp.size();

        // tn
        realNotBuyUserProductTmp = new HashSet<>(realNotBuyUserProduct);
        realNotBuyUserProductTmp.retainAll(predNotBuyUserProductTmp);
        int userProductTn = realNotBuyUserProductTmp.size();

        System.out.println("userProductTp = " + userProductTp + ", userProductFn = " + userProductFn + ", userProductFp = " + userProductFp + ", userProductTn = " + userProductTn);
        double userProductPrecision = userProductTp * 1.0 / (userProductTp + userProductFp);
        double userProductRecall = userProductTp * 1.0 / (userProductTp + userProductFn);
        System.out.println("userProductPrecision = " + userProductPrecision + ", userProductRecall = " + userProductRecall);
        double f12 = (5 * userProductPrecision * userProductRecall) / (3 * userProductPrecision + 2 * userProductRecall);
        System.out.println("f12 = " + f12);

        double score = 0.4 * f11 + 0.6 * f12;
        System.out.println("score = " + score);

        session.stop();
        sc.stop();
    }
}

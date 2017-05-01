package contest.jd.train;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.DenseVector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.FileWriter;
import java.util.*;

/**
 * Created by wanghl on 17-4-16.
 */
public class LRTrain {
    private static final Logger log = Logger.getLogger(LRTrain.class);
    private static final String filepath = "hdfs://big-data1:9000/jd/0417/";

    public static void main(String[] args) throws Exception {
        long CurrentTime = System.currentTimeMillis();
        SparkConf conf = new SparkConf().setAppName("jd-contest").setMaster("spark://big-data1:7077").setJars(new String[]{"/home/wanghl/IdeaProjects/jd-contest/target/jd-contest-1.0.jar"});
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("warn");
        SparkSession session = SparkSession.builder().master("spark://big-data1:7077").appName("jd-contest").config("spark.logConf","true").config("spark.logLevel","warn").getOrCreate();

        Dataset<Row> trainData = session.read().option("header","true").option("inferSchema","true").csv(filepath + "train.csv");
        System.out.println("train data has " + trainData.count() + " rows");
        System.out.println(trainData.columns().length);

        Dataset<Row> testDatat = session.read().option("header","true").option("inferSchema","true").csv(filepath + "test.csv");
        System.out.println("test data has " + testDatat.count() + " rows");
        System.out.println(testDatat.columns().length);

        Dataset<Row> predictionData = session.read().option("header","true").option("inferSchema","true").csv(filepath + "prediction.csv");
        System.out.println("prediction data has " + predictionData.count() + " rows");
        System.out.println(predictionData.columns().length);

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
        features.add("bad_comment_rate");
        for(int i = 4; i <= 16; i++){
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
        Dataset<Row> train = assembler.transform(trainData);
        Dataset<Row> test = assembler.transform(testDatat);

        System.out.println("Train = " + train.count()+" Test = " + test.count());
        train.cache();

        LogisticRegression lor = new LogisticRegression().setFeaturesCol("features").setLabelCol("buy").setRegParam(0.0).setElasticNetParam(1.0).setMaxIter(100).setTol(1E-6).setFitIntercept(true).setThreshold(0.99);
        LogisticRegressionModel model = lor.fit(train);

        Dataset<Row> testPredictions = model.transform(test);
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();
        evaluator.setLabelCol("buy");
        double accuracy = evaluator.evaluate(testPredictions);
        System.out.println("Test accuracy = " + accuracy);
        System.out.println(testPredictions.count());

        testPredictions.show(5);

        Dataset<Row> testResult = testPredictions.select("user_id", "sku_id", "buy", "prediction");
        System.out.println(testResult.count());
        List<Row> testRows = testResult.takeAsList((int) testResult.count());
        System.out.println(testRows.size());

        Set<Integer> realBuyAndPredBuyUser = new HashSet<>();    // tp
        Set<Integer> realBuyAndPredNotBuyUser = new HashSet<>(); // fn
        Set<Integer> notBuyAndPredBuyUser = new HashSet<>();     // fp
        Set<Integer> notBuyAndPredNotBuyuUser = new HashSet<>(); // tn

        Set<String> realBuyUserProduct = new HashSet<>();
        Set<String> realNotBuyUserProduct = new HashSet<>();
        Set<String> predBuyUserProduct = new HashSet<>();
        Set<String> predNotBuyUserProduct = new HashSet<>();

        int tp = 0;
        int fn = 0;
        int fp = 0;
        int tn = 0;
        for(Row row : testRows){
            int userId = row.getAs("user_id");
            int skuId = row.getAs("sku_id");
            int buy = row.getAs("buy");
            double prediction1 = row.getAs("prediction");
            int prediction = (int)prediction1;

            String key = userId + "_" + skuId;

            if(1 == buy && 1 == prediction){
                ++tp;
                realBuyAndPredBuyUser.add(userId);
            }
            else if(1 == buy && 0 == prediction){
                ++fn;
                realBuyAndPredNotBuyUser.add(userId);
            }
            else if(0 == buy && 1 == prediction){
                ++fp;
                notBuyAndPredBuyUser.add(userId);
            }
            else {
                ++tn;
                notBuyAndPredNotBuyuUser.add(userId);
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
        int userTp = realBuyAndPredBuyUser.size();
        int userFn = realBuyAndPredNotBuyUser.size();
        int userFp = notBuyAndPredBuyUser.size();
        int userTn = notBuyAndPredNotBuyuUser.size();
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

        Dataset<Row> prediction = assembler.transform(predictionData);
        Dataset<Row> predictionResult = model.transform(prediction);
        Dataset<Row> predictIdResult = predictionResult.select("user_id", "sku_id", "probability", "prediction");
        List<Row> predictionBuyList = predictIdResult.takeAsList((int) testResult.count());
        CSVWriter writer = new CSVWriter( new FileWriter("/home/wanghl/jd_contest/0418/result.csv"), ',', '\0');
        Map<String, String> resultMap = new HashMap<>();
        Map<String, Double> userProbMap = new HashMap<>();
        writer.writeNext("user_id", "sku_id");
        for(Row row : predictionBuyList){
            int userId = row.getAs("user_id");
            String userIdStr = String.valueOf(userId);
            int skuId = row.getAs("sku_id");
            double probability = ((DenseVector)row.getAs("probability")).apply(0);
            double pred1 = row.getAs("prediction");
            int pred = (int)pred1;
            if(1 == pred){
                if(!userProbMap.containsKey(userIdStr)){
                    userProbMap.put(userIdStr, probability);
                    resultMap.put(userIdStr, String.valueOf(skuId));
                }
                else {
                    if(probability > userProbMap.get(userIdStr)){
                        resultMap.put(userIdStr, String.valueOf(skuId));
                    }
                }
            }
        }

        for(Map.Entry<String, String> entry : resultMap.entrySet()){
            writer.writeNext(entry.getKey(), entry.getValue());
        }

        session.stop();
        sc.stop();
        writer.close();

        System.out.println( "The program spent time is: " + ( System.currentTimeMillis() - CurrentTime ) /60000 );
        System.out.println( "--------------------------------------run completely---------------------------------------------" );
    }
}

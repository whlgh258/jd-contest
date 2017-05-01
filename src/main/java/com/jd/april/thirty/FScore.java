package com.jd.april.thirty;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.log4j.Logger;

import java.io.FileReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by wanghl on 17-4-30.
 */
public class FScore {
    private static final Logger log = Logger.getLogger(FScore.class);

    public static void main (String[] args) throws Exception {
        String path = "/home/wanghl/jd_contest/0501/";
//        String[] names = new String[]{path + "dfpt1.csv", path + "dfpt2.csv", path + "dfpt3.csv", path + "dfpt4.csv"};
//        String[] names = new String[]{path + "dfpv1.csv", path + "dfpv2.csv", path + "dfpv3.csv", path + "dfpv4.csv"};
//        String[] names = new String[]{path + "dfptest1.csv", path + "dfptest2.csv", path + "dfptest3.csv", path + "dfptest4.csv"};
        String[] names = new String[]{path + "dfpPredict1.csv", path + "dfpPredict2.csv", path + "dfpPredict3.csv", path + "dfpPredict4.csv"};
//        String[] names = new String[]{"finalPredictResult.csv"};
        for(String name : names){
            log.info("file: " + name);
            Set<String> userTpSet = new HashSet<>();
            Set<String> userFnSet = new HashSet<>();
            Set<String> userFpSet = new HashSet<>();
            Set<String> userTnSet = new HashSet<>();

            int tp = 0;
            int fn = 0;
            int fp = 0;
            int tn = 0;

            CSVReader reader = new CSVReader(new FileReader(name));
            List<String[]> lines = reader.readAll();
            for(String[] line : lines){
                if(line[0].contains("id")){
                    continue;
                }

                String userId = line[0];
                String skuId = line[1];
                String buy = line[2];
                String predict = line[3];
                if("0".equals(buy) && "0".equals(predict)){
                    // tp
                    ++tp;
                    userTpSet.add(userId);
                }
                else if("0".equals(buy) && !"0".equals(predict)){
                    // fn
                    ++fn;
                    userFnSet.add(userId);
                }
                else if(!"0".equals(buy) && "0".equals(predict)){
                    // fp
                    ++fp;
                    userFpSet.add(userId);
                }
                else {
                    // tn
                    ++tn;
                    userTnSet.add(userId);
                }
            }

            int userTp = userTpSet.size();
            int userFn = userFnSet.size();
            int userFp = userFpSet.size();
            int userTn = userTnSet.size();

            log.info("user tp: " + userTp + ", user fn: " + userFn + ", user fp: " + userFp + ", user tn: " + tn);

            double userPrecision = 0.0;
            if(userTp + userFp > 0){
                userPrecision = userTp * 1.0 / (userTp + userFp);
            }
            double userRecall = userTp * 1.0 / (userTp + userFn);
            double f11 = 0.0;
            log.info("user precision: " + userPrecision + ", user recall: " + userRecall);
            if(userPrecision > 0 || userRecall > 0){
                f11 = 6 * userPrecision * userRecall / (userPrecision + 5 * userRecall);
            }

            log.info("tp: " + tp + ", fn: " + fn + ", fp: " + fp + ", tn: " + tn);

            double precision = 0.0;
            if(tp + fp > 0){
                precision = tp * 1.0 / (tp + fp);
            }
            double recall = tp * 1.0 / (tp + fn);
            log.info("precision: " + precision + ", recall: " + recall);
            double f12 = 0.0;
            if(precision > 0 || recall > 0){
                f12 = 5 * precision * recall / (3 * precision + 2 * recall);
            }

            double f = 0.4 * f11 + 0.6 * f12;

            log.info("f11: " + f11);
            log.info("f12: " + f12);
            log.info("f: " + f);
        }
    }
}

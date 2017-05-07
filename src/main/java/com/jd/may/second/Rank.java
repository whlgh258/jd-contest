package com.jd.may.second;

import java.util.*;

/**
 * Created by wanghl on 17-5-6.
 */
public class Rank {
    public static Map<String, Integer> rank(Map<String, Map<String, Double>> map, String key) {
        TreeMap<Double, String> tree = new TreeMap<>(new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                if(o1 < o2){
                    return 1;
                }
                else {
                    return -1;
                }
            }
        });

        for(Map.Entry<String, Map<String, Double>> entry : map.entrySet()){
            tree.put(entry.getValue().get(key), entry.getKey());
        }


        Map<String, Integer> rank = new LinkedHashMap<>();
        int i = 1;
        for(Map.Entry<Double, String> entry : tree.entrySet()){
            rank.put(entry.getValue(), i++);
        }

        return rank;
    }

    public static Map<String, Integer> rank(Map<String, Double> map) {
        TreeMap<Double, String> tree = new TreeMap<>(new Comparator<Double>() {
            @Override
            public int compare(Double o1, Double o2) {
                if(o1 < o2){
                    return 1;
                }
                else {
                    return -1;
                }
            }
        });

        for(Map.Entry<String, Double> entry : map.entrySet()){
            tree.put(entry.getValue(), entry.getKey());
        }


        Map<String, Integer> rank = new LinkedHashMap<>();
        int i = 1;
        for(Map.Entry<Double, String> entry : tree.entrySet()){
            rank.put(entry.getValue(), i++);
        }

        return rank;
    }
}

package jingdong.contest;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by wanghl on 17-4-10.
 */
public class ProductValidate {
    public static void main(String[] args) throws Exception {
        Set<String> actionProduct = new HashSet<>();
        Set<String> product = new HashSet<>();

        String path = "/home/wanghl/jd_contest/";
        String[] filenames = {"action_02.csv", "action_03.csv", "action_04.csv"};
        for(String filename : filenames) {
            BufferedReader br = new BufferedReader(new FileReader(path + filename));
            String line = null;
            while(null != (line = br.readLine())) {
                if (line.contains("id")) {
                    continue;
                }
                String[] tokens = line.split(",");
                int skuId = Integer.parseInt(tokens[1].trim());
                actionProduct.add(skuId + "");
            }
        }

        BufferedReader br = new BufferedReader(new FileReader("/home/wanghl/jd_contest/product.csv"));
        String line = null;
        while(null != (line = br.readLine())) {
            if (line.contains("id")) {
                continue;
            }
            String[] tokens = line.split(",");
            int productId = Integer.parseInt(tokens[0].trim());
            product.add(productId + "");
        }

        System.out.println("action product size: " + actionProduct.size());
        System.out.println("product size: " + product.size());

        actionProduct.retainAll(product);
        System.out.println("insection size: " + actionProduct.size());
    }
}

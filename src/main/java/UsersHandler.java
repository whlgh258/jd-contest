
import au.com.bytecode.opencsv.CSVReader;

import java.io.*;
import java.util.*;

/**
 * Created by wanghl on 17-4-2.
 */
public class UsersHandler {
    public static void main(String[] args) throws Exception {
        Reader reader = new InputStreamReader(new FileInputStream("/home/wanghl/jd/users.csv"), "gb2312");
        CSVReader csvReader = new CSVReader(reader);
        List<String[]> lines = csvReader.readAll();
        Set<String> set = new HashSet<>();
        for(int i = 1; i< lines.size(); i++){
            String[] tokens = lines.get(i);
            String userId = tokens[0];
            set.add(userId);
        }

        System.out.println(set.size());

        /*reader = new InputStreamReader(new FileInputStream("/home/wanghl/jd/products.csv"), "gb2312");
        csvReader = new CSVReader(reader);
        lines = csvReader.readAll();
        set.clear();
        for(int i = 1; i < lines.size(); i++){
            String[] tokens = lines.get(i);
            String productId = tokens[0];
            set.add(productId);
        }

        System.out.println(set.size());*/

        for(String s : set){
            System.out.println(s);
        }
    }
}

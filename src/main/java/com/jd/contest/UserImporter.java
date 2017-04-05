package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;
import org.codehaus.plexus.util.StringUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-4.
 */
public class UserImporter {
    private static final Logger log =org.apache.log4j.Logger.getLogger(UserImporter.class);

    public static void main(String[] args) throws Exception {
        String filename = "/home/wanghl/jd_contest/user.csv";

        BufferedReader br = null;
        Connection conn = DBConnection.getConnection();
        List<Map<String, Object>> rows = new ArrayList<>();
        try {
            br = new BufferedReader(new FileReader(filename));
            String line = null;
            while(null != (line = br.readLine())){
                if(line.contains("id")){
                    continue;
                }
                String[] tokens = line.split(",");
                int userId = Integer.parseInt(tokens[0].trim());
                int age = 0;
                String ageStr = tokens[1].trim();
                try{
                    age = Integer.parseInt(ageStr);
                }
                catch (Exception e){
                    age = 0;
                }
                if(age < 0){
                    age = 0;
                }

                int sex = 2;
                String sexStr = tokens[2].trim();
                try{
                    sex = Integer.parseInt(sexStr);
                }
                catch (Exception e){
                    sex = 2;
                }
                int userLevel = Integer.parseInt(tokens[3].trim());

                LocalDate regDate = null;
                if(!"NULL".equalsIgnoreCase(tokens[4].trim())){
                    regDate = LocalDate.parse(tokens[4].trim());
                }

                Date date = null;
                if(null != regDate){
                    date = Date.valueOf(regDate);
                }

                Map<String, Object> row = new HashMap<>();
                row.put("user_id", userId);
                row.put("age", age);
                row.put("sex", sex);
                row.put("user_level", userLevel);
                row.put("reg_date", date);

                rows.add(row);
            }

            DBOperation.insert(conn, "jd_contest.user", rows);
        }
        catch (Exception e){
            log.error(e.getMessage(), e);
        }
        finally {
            br.close();
            DBConnection.close(conn);
        }
    }

}

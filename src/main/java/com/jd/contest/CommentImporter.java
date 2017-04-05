package com.jd.contest;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.Date;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-4.
 */
public class CommentImporter {
    private static final Logger log = Logger.getLogger(CommentImporter.class);

    public static void main(String[] args) throws Exception {
        String filename = "/home/wanghl/jd_contest/comment.csv";

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
                int productId = Integer.parseInt(tokens[1].trim());
                int commentNum = 0;
                String commentNumStr = tokens[2].trim();
                try{
                    commentNum = Integer.parseInt(commentNumStr);
                }
                catch (Exception e){
                    commentNum = 0;
                }
                if(commentNum < 0){
                    commentNum = 0;
                }

                int hasBadComment = 0;
                String hasBadCommentStr = tokens[3].trim();
                try{
                    hasBadComment = Integer.parseInt(hasBadCommentStr);
                }
                catch (Exception e){
                    hasBadComment = 2;
                }

                double badCommentRate = Double.parseDouble(tokens[4].trim());

                LocalDate lastDate = null;
                if(!"NULL".equalsIgnoreCase(tokens[0].trim())){
                    lastDate = LocalDate.parse(tokens[0].trim());
                }

                Date date = null;
                if(null != lastDate){
                    date = Date.valueOf(lastDate);
                }

                Map<String, Object> row = new HashMap<>();
                row.put("sku_id", productId);
                row.put("comment_num", commentNum);
                row.put("has_bad_comment", hasBadComment);
                row.put("bad_comment_rate", badCommentRate);
                row.put("last_date", date);

                rows.add(row);
            }

            DBOperation.insert(conn, "jd_contest.comment", rows);
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

package jingdong.contest.horizon;

import java.sql.Connection;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.Map;

import multithreads.DBConnection;
import multithreads.DBOperation;
import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-4-10.
 */
public class CreateHorizonTable {
    private static final Logger log =org.apache.log4j.Logger.getLogger(CreateHorizonTable.class);

    public static void main(String[] args) {
        LinkedHashMap<String, String> fieldsMap = new LinkedHashMap<>();

        fieldsMap.put("id", "int not null auto_increment");
        fieldsMap.put("user_id", "int default 0");
        fieldsMap.put("sku_id", "int default 0");
        fieldsMap.put("age", "int default 0");
        fieldsMap.put("sex", "int default 0");
        fieldsMap.put("user_level", "int default 0");
        fieldsMap.put("reg_date", "date");
        fieldsMap.put("attr1", "int default 0");
        fieldsMap.put("attr2", "int default 0");
        fieldsMap.put("attr3", "int default 0");
        fieldsMap.put("cate", "int default 0");
        fieldsMap.put("brand", "int default 0");
        fieldsMap.put("comment_num", "int default 0");
        fieldsMap.put("has_bad_comment", "int default 0");
        fieldsMap.put("bad_comment_rate", "double default 0.0");
        fieldsMap.put("last_comment_date", "date");

        LocalDate start = LocalDate.parse("2016-02-01");
        LocalDate end = LocalDate.parse("2016-04-15");
        LocalDate targetDate = LocalDate.parse("2016-04-16");
        for(LocalDate date = start; date.isBefore(end.plusDays(1)); date = date.plusDays(1)){
            int diff = targetDate.getDayOfYear() - date.getDayOfYear();
            fieldsMap.put("click_" + diff, "int default 0");
            fieldsMap.put("detail_" + diff, "int default 0");
            fieldsMap.put("cart_" + diff, "int default 0");
            fieldsMap.put("cart_delete_" + diff, "int default 0");
            fieldsMap.put("buy_" + diff, "int default 0");
            fieldsMap.put("follow_" + diff, "int default 0");
        }

        DBOperation.createtable("user_product_horizon", fieldsMap, "primary key(id)", "");
    }
}


















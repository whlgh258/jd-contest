package contest.jd;

import multithreads.DBOperation;
import org.apache.log4j.Logger;

import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-10.
 */
public class CreateHorizonTable {
    private static final Logger log = Logger.getLogger(CreateHorizonTable.class);

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
        fieldsMap.put("bad_comment_rate", "int default 0");
        fieldsMap.put("last_comment_date", "date");

        LocalDate start = LocalDate.parse("2016-02-01");
        LocalDate end = LocalDate.parse("2016-04-20");
        int i = 0;
        for(LocalDate date = end; date.isAfter(start); date = date.minusDays(5)){
            ++i;
            fieldsMap.put("click_" + i, "int default 0");
            fieldsMap.put("detail_" + i, "int default 0");
            fieldsMap.put("cart_" + i, "int default 0");
            fieldsMap.put("cart_delete_" + i, "int default 0");
            fieldsMap.put("buy_" + i, "int default 0");
            fieldsMap.put("follow_" + i, "int default 0");
            fieldsMap.put("contains_weekend_" + i, "int default 0");
        }

        String sql = "select distinct model_id from action_1 order by model_id";
        List<Map<String, Object>> result = DBOperation.queryBySql(sql);
        for(Map<String, Object> row : result){
            int modelId = (int) row.get("model_id");
            if(modelId > 0){
                fieldsMap.put("model_" + modelId, "int default 0");
            }
        }

        DBOperation.createtable("user_action_segment", fieldsMap, "primary key(id)", "");
    }
}


















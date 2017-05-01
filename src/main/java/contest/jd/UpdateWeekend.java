package contest.jd;

import org.apache.log4j.Logger;

/**
 * Created by wanghl on 17-4-16.
 */
public class UpdateWeekend {
    private static final Logger log = Logger.getLogger(UpdateWeekend.class);

    public static void main(String[] args) {
        String sql = "update user_action_segment set contains_weekend_1=1,contains_weekend_2=1,contains_weekend_3=1," +
                     "contains_weekend_4=1,contains_weekend_5=1,contains_weekend_6=1,contains_weekend_7=1,contains_weekend_8=1," +
                     "contains_weekend_9=1,contains_weekend_10=1,contains_weekend_11=1,contains_weekend_12=1,contains_weekend_13=1," +
                     "contains_weekend_14=1,contains_weekend_15=1,contains_weekend_16=1";
    }
}

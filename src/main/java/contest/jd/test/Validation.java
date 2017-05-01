package contest.jd.test;

import au.com.bytecode.opencsv.CSVReader;
import org.apache.log4j.Logger;

import java.io.FileReader;

/**
 * Created by wanghl on 17-4-17.
 */
public class Validation {
    private static final Logger log = Logger.getLogger(Validation.class);

    public static void main(String[] args) throws Exception {
        CSVReader reader = new CSVReader(new FileReader("/home/wanghl/jd_contest/0416/test.csv"));
        int buy3 = 0;
        String[] line = null;
        int i = 1;
        while(null != (line = reader.readNext())){
            if(1 == i){
                continue;
            }
            ++i;
            int buy = Integer.parseInt(line[184]);
            if(buy > 0){
                ++buy3;
            }
        }

        log.info(buy3);
    }
}

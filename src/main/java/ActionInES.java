import org.apache.log4j.Logger;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Created by wanghl on 17-4-3.
 */
public class ActionInES {
    private static final Logger log = Logger.getLogger(ActionInES.class);
    public static void main(String[] args) throws UnknownHostException {
        Connection conn = DBConnection.getConnection();
        Settings settings = Settings.settingsBuilder().put("cluster.name", "elasticsearch").build();
        TransportClient client = TransportClient.builder().settings(settings).build();
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("big-data2"), 9300));
        client.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName("big-data3"), 9300));

        for(int i = 0; i < 103616; i += 5000){
            String sql = "select a.user_id,a.sku_id,model_id,type,a.cate as action_cate,a.brand as action_brand,time,date_format(time,'%Y-%m-%d') as date,age,sex,user_lv_cd,user_reg_dt,attr1,attr2,attr3,p.cate as product_cate,p.brand as product_brand from jd.action a left join jd.users u on a.user_id=u.user_id left join jd.products p on a.sku_id=p.sku_id where a.user_id>" + i + " and a.user_id<=" + (i + 5000);
            log.info(sql);
            List<Map<String, Object>> result = DBOperation.queryBySql(conn, sql);
            log.info("result size: " + result.size());
            int j = 0;
            BulkRequestBuilder bulkRequest = client.prepareBulk();
            for(Map<String, Object> row : result){
                ++j;
                bulkRequest.add(client.prepareIndex("action", "action").setSource(row));
                if(j % 10000 == 0){
                    bulkRequest.execute().actionGet();
                    bulkRequest = client.prepareBulk();
                }
            }
            bulkRequest.execute().actionGet();
        }
    }
}

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.asyncsql.MySQLClient;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yizmao
 * @date 18-6-12 上午10:43
 */
public class BatchTest {
    public static void main(String[] args) {
        Vertx vertx = Vertx.vertx();
        /*JsonObject mySQLClientConfig = new JsonObject()
            .put("host", "127.0.0.1")
            .put("username","root")
            .put("password","123456")
            .put("database","demo");*/
        JsonObject mySQLClientConfig = new JsonObject()
            .put("url", "jdbc:mysql://127.0.0.1:3306/demo")
            .put("user","root")
            .put("password","123456");
        SQLClient mySQLClient = JDBCClient.createShared(vertx, mySQLClientConfig);

        mySQLClient.getConnection(res -> {
            if (res.succeeded()){
                SQLConnection connection = res.result();
                connection.setAutoCommit(true,auto -> {
                    List<JsonArray> batch = new ArrayList<>();
                    batch.add(new JsonArray().addNull().add("joe").add("111"));
                    batch.add(new JsonArray().addNull().add("jane").add("222"));
                    String sql = "INSERT INTO users(id,username,password) VALUES (?,?,?)";
                    connection.batchWithParams(sql,batch,batchRes -> {
                        if (batchRes.succeeded()){
                            batchRes.result().forEach(integer -> System.out.println(integer));
                        }
                    });
                    connection.close();
                });
            }
        });
    }
}

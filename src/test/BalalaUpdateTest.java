import io.github.balala.Balala;
import io.github.balala.core.ResultKey;
import io.github.balala.page.Page;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.jdbc.JDBCClient;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import lombok.extern.slf4j.Slf4j;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static io.github.balala.Balala.select;

/**
 * @author yizmao
 * @date 18-6-11 下午1:17
 */
@RunWith(VertxUnitRunner.class)
@Slf4j
public class BalalaUpdateTest {


    @BeforeClass
    public static void before(){
        Vertx vertx = Vertx.vertx();
       /* JsonObject mySQLClientConfig = new JsonObject()
            .put("host", "127.0.0.1")
            .put("username","root")
            .put("password","123456")
            .put("database","demo");
        SQLClient mySQLClient = MySQLClient.createShared(vertx, mySQLClientConfig);*/
        JsonObject mySQLClientConfig = new JsonObject()
            .put("url", "jdbc:mysql://127.0.0.1:3306/demo")
            .put("user","root")
            .put("password","123456");
        SQLClient mySQLClient = JDBCClient.createShared(vertx, mySQLClientConfig);
        Balala.open(mySQLClient);
    }

    @Test
    public void save() {
        Future<ResultKey> save = new User("user", "pass").save();
        if (save.succeeded()){
            ResultKey result = save.result();
        }
    }

    @Test
    public void update(){
        User user = new User("111", "pass");
        user.setId(1);
        Future<Integer> update = user.update();
        if (update.succeeded()){
            System.out.println(update.result());
        }
    }

    @Test
    public void updateById(){
        User user = new User("张","卫星");
        Future<Integer> future = user.updateById(1);
    }

    @Test
    public void delete(){
        Future<Integer> user = new User("111","pass").delete();
    }

    @Test
    public void batch(){
        List<User> userList = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            userList.add( new User("111","pass"));
        }

        Future<Integer> future = Balala.saveBatch(userList,User.class);

        System.out.println(future.succeeded());
        while (!future.succeeded()){

        }
    }

    @Test
    public void selectList(){
        Future<List<JsonObject>> all = select().from(User.class).all();
        while (!all.succeeded()){
        }
        all.result().forEach(entries -> System.out.println(entries.encode()));
    }

    @Test
    public void selectCount(){
        Future<Long> count = select().from(User.class).count();
        while (!count.succeeded()){
        }
        System.out.println(count.result());
    }

    @Test
    public void selectByIds(){
        Future<List<JsonObject>> userFuture = select().from(User.class).byIds(2,3);
        while (!userFuture.succeeded()){
        }
        System.out.println(userFuture.result());
    }

    @Test
    public void selectById(){
        Future<JsonObject> userFuture = select().from(User.class).byId(2);
        while (!userFuture.succeeded()){
        }
        System.out.println(userFuture.result());
    }

    @Test
    public void like(){
        Future<List<JsonObject>> likeFuture = select().from(User.class).like(User::getUsername, "%jo%").all();
        while (!likeFuture.succeeded()){
        }
        System.out.println(likeFuture.result());
    }

    @Test
    public void page(){
        Future<Page<JsonObject>> pageFuture = select().from(User.class).order("id desc").page(1, 3);
        while (!pageFuture.succeeded()){
        }
        System.out.println(pageFuture);
    }
}

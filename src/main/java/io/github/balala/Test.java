package io.github.balala;

import io.vertx.core.json.JsonObject;

/**
 * @author yizmao
 * @date 18-6-12 上午11:51
 */
public class Test {
    public static void main(String[] args) {
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("username","1111");
        jsonObject.put("password","2222");

        User user = new User();
        user = jsonObject.mapTo(user.getClass());

        System.out.println(user.getPassword());
    }
}

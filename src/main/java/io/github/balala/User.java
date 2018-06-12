package io.github.balala;

import lombok.Getter;
import lombok.Setter;

/**
 * @author yizmao
 * @date 18-6-11 下午1:27
 */
public class User extends Model {
    @Getter
    @Setter
    private Integer id;

    @Getter
    @Setter
    private String username;

    @Getter
    @Setter
    private String password;

    public User(String username, String password) {
        this.username = username;
        this.password = password;
    }

    public User() {
    }
}

package io.github.balala.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * @author biezhi
 * @date 2018/3/16
 */
@Getter
@AllArgsConstructor
public enum ErrorCode {

    SQLCLIENT_IS_NULL(1000, "SQLCLIENT instance is not configured successfully, please check your database configuration :)"),
    FROM_NOT_NULL(1001, "from class cannot be null, please check :)"),
    BALALA_IS_NULL(1002,"BALALA instance is not init");
    private Integer code;
    private String  msg;

}

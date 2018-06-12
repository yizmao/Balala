package io.github.balala;

import io.github.balala.core.BalalaUpdate;
import io.github.balala.core.ResultKey;
import io.github.balala.core.dml.Delete;
import io.github.balala.core.dml.Select;
import io.github.balala.core.dml.Update;
import io.github.balala.core.functions.TypeFunction;
import io.github.balala.dialect.Dialect;
import io.github.balala.dialect.MySQLDialect;
import io.github.balala.enums.ErrorCode;
import io.github.balala.exception.AnimaException;
import io.github.balala.utils.AnimaUtils;
import io.vertx.core.Future;
import io.vertx.ext.sql.SQLClient;
import lombok.Getter;
import lombok.Setter;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author yizmao
 * @date 18-6-11 上午11:03
 */
public class Balala {
    /**
     * sqlClient instance
     */
    @Getter
    @Setter
    private SQLClient sqlClient;

    /**
     * Database dialect, default by MySQL
     */
    @Getter
    @Setter
    private Dialect dialect = new MySQLDialect();

    /**
     * Global table prefix
     */
    @Getter
    @Setter
    private String tablePrefix;

    @Getter
    private boolean useSQLLimit = true;

    private static Balala instance;

    public static Balala me() {
        if (null == instance.sqlClient) {
            throw new AnimaException(ErrorCode.SQLCLIENT_IS_NULL);
        }
        return instance;
    }

    /**
     * Create anima with SQLClientImpl
     *
     * @param sqlClient SQLClientImpl instance
     * @return Balala
     */
    public static Balala open(SQLClient sqlClient) {
        Balala anima = new Balala();
        anima.setSqlClient(sqlClient);
        instance = anima;
        return anima;
    }


    /**
     * Open a query statement.
     *
     * @return Select
     */
    public static Select select() {
        return new Select();
    }

    /**
     * Open a query statement and specify the query for some columns.
     *
     * @param columns column names
     * @return Select
     */
    public static Select select(String columns) {
        return new Select(columns);
    }

    /**
     * Set the query to fix columns with lambda
     *
     * @param functions column lambdas
     * @param <T>
     * @param <R>
     * @return Select
     */
    @SafeVarargs
    public static <T extends Model, R> Select select(TypeFunction<T, R>... functions) {
        return select(Arrays.stream(functions).map(AnimaUtils::getLambdaColumnName).collect(Collectors.joining(", ")));
    }


    /**
     * Set the global table prefix, like "t_"
     *
     * @param tablePrefix table prefix
     * @return Anima
     */
    public Balala tablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
        return this;
    }

    /**
     * Open an update statement.
     *
     * @return Update
     */
    public static Update update() {
        return new Update();
    }

    /**
     * Open a delete statement.
     *
     * @return Delete
     */
    public static Delete delete() {
        return new Delete();
    }

    /**
     * Save a model
     *
     * @param <T>
     * @param model database model
     * @return ResultKey
     */
    public static <T extends Model> Future<ResultKey> save(T model) {
        return model.save();
    }

    /**
     * Batch save model
     *
     * @param <T>
     * @param models model list
     */
    public static <T extends Model> Future<Integer> saveBatch(List<T> models,Class type) {

        return new BalalaUpdate<T>().saveBatch(models,type);
    }
}

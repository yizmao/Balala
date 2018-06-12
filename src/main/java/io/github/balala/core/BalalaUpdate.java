package io.github.balala.core;

import io.github.balala.Balala;
import io.github.balala.Model;
import io.github.balala.core.functions.TypeFunction;
import io.github.balala.enums.DMLType;
import io.github.balala.enums.ErrorCode;
import io.github.balala.exception.AnimaException;
import io.github.balala.utils.AnimaUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.sql.SQLClient;
import io.vertx.ext.sql.SQLConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * update by model
 *
 * @author yizmao
 * @date 18-6-11 上午11:25
 *
 */
@Slf4j
public class BalalaUpdate<T extends Model> {

    /**
     * Java Model, a table of corresponding databases.
     */
    private Class<T> modelClass;

    /**
     * Model table name
     */
    private String tableName;

    /**
     * Primary key column name
     */
    private String primaryKeyColumn;

    /**
     * Storage condition clause.
     */
    private StringBuilder conditionSQL = new StringBuilder();

    /**
     * A column that stores updates.
     */
    private Map<String, Object> updateColumns = new LinkedHashMap<>(8);

    /**
     * Storage parameter list
     */
    private JsonArray paramValues = new JsonArray();

    /**
     * @see DMLType
     */
    private DMLType dmlType;

    public BalalaUpdate(Class<T> modelClass) {
        this.parse(modelClass);
    }


    public BalalaUpdate() {}

    public BalalaUpdate(DMLType dmlType) {
        this.dmlType = dmlType;
    }
    public BalalaUpdate<T> parse(Class<T> modelClass) {
        this.modelClass = modelClass;
        this.tableName = AnimaCache.getTableName(modelClass);
        this.primaryKeyColumn = AnimaCache.getPKColumn(modelClass);
        return this;
    }

    /**
     * Save a model
     *
     * @param model model instance
     * @param <S>
     * @return ResultKey
     */
    public <S extends Model> Future<ResultKey> save(S model) {
        Future<ResultKey> future = Future.future();
        String       sql             = this.buildInsertSQL(model);
        JsonArray columnValueList = AnimaUtils.toColumnValues(model, true);
        SQLClient sqlClient = getSqlClient();
        sqlClient.updateWithParams(sql,columnValueList,res -> {
             if (res.succeeded()) {
                 Object key = res.result().getKeys().getValue(0);
                 future.complete(new ResultKey(key));
             }else {
                 future.fail(res.cause());
             }
        });
        return future;
    }

    /**
     * Update operation
     *
     * @return affect the number of rows
     */
    public Future<Integer> update() {
        this.beforeCheck();
        String       sql             = this.buildUpdateSQL(null, updateColumns);
        JsonArray columnValueList = new JsonArray();
        updateColumns.forEach((key, value) -> columnValueList.add(value));
        columnValueList.addAll(paramValues);
        return this.execute(sql, columnValueList);
    }

    /**
     * Update a model
     *
     * @param <S>
     * @param model model instance
     * @return affect the number of rows
     */
    public <S extends Model> Future<Integer> updateByModel(S model) {
        this.beforeCheck();

        Object primaryKey = AnimaUtils.getAndRemovePrimaryKey(model);

        StringBuilder sql = new StringBuilder(this.buildUpdateSQL(model, null));

        JsonArray columnValueList = AnimaUtils.toColumnValues(model, false);
        if (null != primaryKey) {
            sql.append(" WHERE ").append(this.primaryKeyColumn).append(" = ?");
            columnValueList.add(primaryKey);
        }
        return this.execute(sql.toString(), columnValueList);
    }

    /**
     * Update model by primary key
     *
     * @param id primary key value
     * @return affect the number of rows, normally it's 1.
     */
    public Future<Integer> updateById(Serializable id) {
        this.where(primaryKeyColumn, id);
        return this.update();
    }

    /**
     * Update model by primary key
     *
     * @param <S>
     * @param model model instance
     * @param id    primary key value
     * @return affect the number of rows, normally it's 1.
     */
    public <S extends Model> Future<Integer> updateById(S model, Serializable id) {
        this.where(primaryKeyColumn, id);
        String       sql             = this.buildUpdateSQL(model, null);
        JsonArray columnValueList = AnimaUtils.toColumnValues(model, false);
        columnValueList.add(id);
        return this.execute(sql, columnValueList);
    }

    /**
     * Delete model
     *
     * @param model model instance
     * @param <S>
     * @return affect the number of rows
     */
    public <S extends Model> Future<Integer> deleteByModel(S model) {
        this.beforeCheck();
        String       sql             = this.buildDeleteSQL(model);
        JsonArray columnValueList = AnimaUtils.toColumnValues(model, false);
        return this.execute(sql, columnValueList);
    }

    /**
     * Update columns set value
     *
     * @param column column name
     * @param value  column value
     * @return AnimaQuery
     */
    public BalalaUpdate<T> set(String column, Object value) {
        updateColumns.put(column, value);
        return this;
    }


    /**
     * Update the model sets column.
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <S>
     * @param <R>
     * @return
     */
    public <S extends Model, R> BalalaUpdate<T> set(TypeFunction<S, R> function, Object value) {
        return this.set(AnimaUtils.getLambdaColumnName(function), value);
    }


    /**
     * where condition, simultaneous setting value
     *
     * @param statement like "age > ?" "name = ?"
     * @param value     column name
     * @return AnimaQuery
     */
    public BalalaUpdate<T> where(String statement, Object value) {
        conditionSQL.append(" AND ").append(statement);
        if (!statement.contains("?")) {
            conditionSQL.append(" = ?");
        }
        paramValues.add(value);
        return this;
    }

    /**
     * Set the column name using lambda, at the same time setting the value, the SQL generated is "column = ?"
     *
     * @param function lambda expressions, use the Model::getXXX
     * @param value    column value
     * @param <S>
     * @param <R>
     * @return AnimaQuery
     */
    public <S extends Model, R> BalalaUpdate<T> where(TypeFunction<S, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        conditionSQL.append(" AND ").append(columnName).append(" = ?");
        paramValues.add(value);
        return this;
    }


    /**
     * Execute sql statement
     *
     * @param sql    sql statement
     * @param columnValue params
     * @return affect the number of rows
     */
    public Future<Integer> execute(String sql,JsonArray columnValue){
        Future<Integer> future = Future.future();
        SQLClient sqlClient = getSqlClient();
        sqlClient.updateWithParams(sql,columnValue,res -> {
            if (res.succeeded()){
                int rows = res.result().getUpdated();
                future.complete(rows);
            }else {
                future.fail(res.cause());
            }
        });
        return future;
    }


    /**
     * Build a insert statement.
     *
     * @param model model instance
     * @param <S>
     * @return insert sql
     */
    private <S extends Model> String buildInsertSQL(S model) {
        SQLParams sqlParams = SQLParams.builder()
            .model(model)
            .modelClass(this.modelClass)
            .tableName(this.tableName)
            .pkName(this.primaryKeyColumn)
            .build();

        return Balala.me().getDialect().insert(sqlParams);
    }

    /**
     * Build a update statement.
     *
     * @param model         model instance
     * @param updateColumns update columns
     * @param <S>
     * @return update sql
     */
    private <S extends Model> String buildUpdateSQL(S model, Map<String, Object> updateColumns) {
        SQLParams sqlParams = SQLParams.builder()
            .model(model)
            .modelClass(this.modelClass)
            .tableName(this.tableName)
            .pkName(this.primaryKeyColumn)
            .updateColumns(updateColumns)
            .conditionSQL(this.conditionSQL)
            .build();

        return Balala.me().getDialect().update(sqlParams);
    }

    /**
     * Build a delete statement.
     *
     * @param model model instance
     * @param <S>
     * @return delete sql
     */
    private <S extends Model> String buildDeleteSQL(S model) {
        SQLParams sqlParams = SQLParams.builder()
            .model(model)
            .modelClass(this.modelClass)
            .tableName(this.tableName)
            .pkName(this.primaryKeyColumn)
            .conditionSQL(this.conditionSQL)
            .build();
        return Balala.me().getDialect().delete(sqlParams);
    }

    private SQLClient getSqlClient(){
        return Balala.me().getSqlClient();
    }

    /**
     * pre check
     */
    private void beforeCheck() {
        if (null == this.modelClass) {
            throw new AnimaException(ErrorCode.FROM_NOT_NULL);
        }
    }

    /**
     * 批量保存
     * @param models
     * @param <T>
     * @return
     */
    public <T extends Model> Future<Integer> saveBatch(List<T> models,Class type){
        this.tableName = AnimaCache.getTableName(type);
        Future future = Future.future();
        T t = models.get(0);
        this.modelClass = type;
        String sql = buildInsertSQL(t);
        List<JsonArray> batchValue = new LinkedList<>();
        models.forEach(model -> {
            JsonArray columnValueList = AnimaUtils.toColumnValues(model, true);
            batchValue.add(columnValueList);
        });
        SQLClient sqlClient = getSqlClient();
        sqlClient.getConnection(res -> {
            if (res.succeeded()){
                SQLConnection connection = res.result();
                connection.setAutoCommit(true, auto -> {
                    if (auto.succeeded()){
                        System.out.println(sql);
                        connection.batchWithParams(sql,batchValue, batchRes -> {
                            batchRes.result().forEach(s -> System.out.println(s));
                            if (batchRes.succeeded()){
                                future.complete(batchRes.result());
                            }else{
                                future.fail(batchRes.cause());
                            }
                        });
                    }else {
                        future.fail(auto.cause());
                    }
                    connection.close();
                });
            }else {
                res.cause().printStackTrace();
                future.fail(res.cause());
            }
        });
        return future;
    }
    /**
     * Clear the battlefield after a database operation.
     *
     */
    private void clean() {
        this.conditionSQL = new StringBuilder();
        this.paramValues.clear();
        this.updateColumns.clear();
    }
}

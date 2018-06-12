package io.github.balala.core;

import io.github.balala.Balala;
import io.github.balala.Model;
import io.github.balala.core.functions.TypeFunction;
import io.github.balala.enums.DMLType;
import io.github.balala.enums.ErrorCode;
import io.github.balala.enums.OrderBy;
import io.github.balala.exception.AnimaException;
import io.github.balala.page.Page;
import io.github.balala.page.PageRow;
import io.github.balala.utils.AnimaUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.sql.SQLClient;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Field;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * query Operational database core class
 * @author yizmao
 * @date 18-6-11 下午3:21
 */
@Slf4j
@NoArgsConstructor
public class BalalaQuery<T extends Model> {
    /**
     * Java Model, a table of corresponding databases.
     */
    private Class<T> modelClass;

    /**
     * Specify a few columns, such as “uid, name, age”
     */
    private String selectColumns;

    /**
     * Primary key column name
     */
    private String primaryKeyColumn;

    /**
     * Model table name
     */
    private String tableName;


    /**
     * Storage parameter list
     */
    private JsonArray paramValues = new JsonArray();

    /**
     * Storage condition clause.
     */
    private StringBuilder conditionSQL = new StringBuilder();

    /**
     * Storage order by clause.
     */
    private StringBuilder orderBySQL = new StringBuilder();

    /**
     * Store the column names to be excluded.
     */
    private List<String> excludedColumns = new ArrayList<>(8);

    /**
     * Do you use SQL for limit operations and use "limit ?" if enabled.
     * The method of querying data is opened by default, and partial database does not support this operation.
     */
    private boolean isSQLLimit;

    /**
     * @see DMLType
     */
    private DMLType dmlType;


    public BalalaQuery(DMLType dmlType) {
        this.dmlType = dmlType;
    }

    public BalalaQuery(Class<T> modelClass) {
        this.parse(modelClass);
    }

    public BalalaQuery<T> parse(Class<T> modelClass) {
        this.modelClass = modelClass;
        this.tableName = AnimaCache.getTableName(modelClass);
        this.primaryKeyColumn = AnimaCache.getPKColumn(modelClass);
        return this;
    }

    /**
     * Sets the query to specify the column.
     *
     * @param columns table column name
     * @return BalalaQuery
     */
    public BalalaQuery<T> select(String columns) {
        if (null != this.selectColumns) {
            throw new AnimaException("Select method can only be called once.");
        }
        this.selectColumns = columns;
        return this;
    }

    /**
     * where condition
     *
     * @param statement like "age > ?" "name = ?"
     * @return BalalaQuery
     */
    public BalalaQuery<T> where(String statement) {
        conditionSQL.append(" AND ").append(statement);
        return this;
    }

    /**
     * where condition, simultaneous setting value
     *
     * @param statement like "age > ?" "name = ?"
     * @param value     column name
     * @return BalalaQuery
     */
    public BalalaQuery<T> where(String statement, Object value) {
        conditionSQL.append(" AND ").append(statement);
        if (!statement.contains("?")) {
            conditionSQL.append(" = ?");
        }
        paramValues.add(value);
        return this;
    }

    /**
     * Set the column name using lambda
     *
     * @param function lambda expressions, use the Model::getXXX
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> where(TypeFunction<T, R> function) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        conditionSQL.append(" AND ").append(columnName);
        return this;
    }

    /**
     * Set the column name using lambda, at the same time setting the value, the SQL generated is "column = ?"
     *
     * @param function lambda expressions, use the Model::getXXX
     * @param value    column value
     * @param <S>
     * @param <R>
     * @return BalalaQuery
     */
    public <S extends Model, R> BalalaQuery<T> where(TypeFunction<S, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        conditionSQL.append(" AND ").append(columnName).append(" = ?");
        paramValues.add(value);
        return this;
    }

    /**
     * Set the where parameter according to model,
     * and generate sql like where where age = ? and name = ?
     *
     * @param model
     * @return BalalaQuery
     */
    public BalalaQuery<T> where(T model) {
        Field[] declaredFields = model.getClass().getDeclaredFields();
        for (Field declaredField : declaredFields) {
            Object value = AnimaUtils.getFieldValue(declaredField, model);
            if (null == value) {
                continue;
            }
            if (declaredField.getType().equals(String.class) && AnimaUtils.isEmpty(value.toString())) {
                continue;
            }
            String columnName = AnimaUtils.toColumnName(declaredField);
            this.where(columnName, value);
        }
        return this;
    }

    /**
     * Equals statement
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> eq(Object value) {
        conditionSQL.append(" = ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "IS NOT NULL" statement
     *
     * @return BalalaQuery
     */
    public BalalaQuery<T> notNull() {
        conditionSQL.append(" IS NOT NULL");
        return this;
    }

    /**
     * generate AND statement, simultaneous setting value
     *
     * @param statement condition clause
     * @param value     column value
     * @return
     */
    public BalalaQuery<T> and(String statement, Object value) {
        return this.where(statement, value);
    }

    /**
     * generate AND statement with lambda
     *
     * @param function column name with lambda
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> and(TypeFunction<T, R> function) {
        return this.where(function);
    }

    /**
     * generate AND statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> and(TypeFunction<T, R> function, Object value) {
        return this.where(function, value);
    }

    /**
     * generate OR statement, simultaneous setting value
     *
     * @param statement like "name = ?" "age > ?"
     * @param value     column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> or(String statement, Object value) {
        conditionSQL.append(" OR (").append(statement);
        if (!statement.contains("?")) {
            conditionSQL.append(" = ?");
        }
        conditionSQL.append(')');
        paramValues.add(value);
        return this;
    }

    /**
     * generate "!=" statement, simultaneous setting value
     *
     * @param columnName column name [sql]
     * @param value      column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> notEq(String columnName, Object value) {
        conditionSQL.append(" AND ").append(columnName).append(" != ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "!=" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> notEq(TypeFunction<T, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.notEq(columnName, value);
    }

    /**
     * generate "!=" statement, simultaneous setting value
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> notEq(Object value) {
        conditionSQL.append(" != ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "!= ''" statement
     *
     * @param columnName column name
     * @return BalalaQuery
     */
    public BalalaQuery<T> notEmpty(String columnName) {
        conditionSQL.append(" AND ").append(columnName).append(" != ''");
        return this;
    }

    /**
     * generate "!= ''" statement with lambda
     *
     * @param function column name with lambda
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> notEmpty(TypeFunction<T, R> function) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.notEmpty(columnName);
    }

    /**
     * generate "!= ''" statement
     *
     * @return BalalaQuery
     */
    public BalalaQuery<T> notEmpty() {
        conditionSQL.append(" != ''");
        return this;
    }

    /**
     * generate "IS NOT NULL" statement
     *
     * @param columnName column name
     * @return
     */
    public BalalaQuery<T> notNull(String columnName) {
        conditionSQL.append(" AND ").append(columnName).append(" IS NOT NULL");
        return this;
    }

    /**
     * generate like statement, simultaneous setting value
     *
     * @param columnName column name
     * @param value      column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> like(String columnName, Object value) {
        conditionSQL.append(" AND ").append(columnName).append(" LIKE ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate like statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> like(TypeFunction<T, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.like(columnName, value);
    }

    /**
     * generate like statement, simultaneous setting value
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> like(Object value) {
        conditionSQL.append(" LIKE ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate between statement, simultaneous setting value
     *
     * @param columnName column name
     * @param a          first range value
     * @param b          second range value
     * @return BalalaQuery
     */
    public BalalaQuery<T> between(String columnName, Object a, Object b) {
        conditionSQL.append(" AND ").append(columnName).append(" BETWEEN ? and ?");
        paramValues.add(a);
        paramValues.add(b);
        return this;
    }

    /**
     * generate between statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param a        first range value
     * @param b        second range value
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> between(TypeFunction<T, R> function, Object a, Object b) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.between(columnName, a, b);
    }

    /**
     * generate between values
     *
     * @param a first range value
     * @param b second range value
     * @return BalalaQuery
     */
    public BalalaQuery<T> between(Object a, Object b) {
        conditionSQL.append(" BETWEEN ? and ?");
        paramValues.add(a);
        paramValues.add(b);
        return this;
    }

    /**
     * generate ">" statement, simultaneous setting value
     *
     * @param columnName table column name [sql]
     * @param value      column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> gt(String columnName, Object value) {
        conditionSQL.append(" AND ").append(columnName).append(" > ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate ">" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> gt(TypeFunction<T, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.gt(columnName, value);
    }

    /**
     * generate ">" statement value
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> gt(Object value) {
        conditionSQL.append(" > ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate ">=" statement value
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> gte(Object value) {
        conditionSQL.append(" >= ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate ">=" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <S extends Model, R> BalalaQuery<T> gte(TypeFunction<S, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.gte(columnName, value);
    }

    /**
     * generate "<" statement value
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> lt(Object value) {
        conditionSQL.append(" < ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "<" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <S extends Model, R> BalalaQuery<T> lt(TypeFunction<S, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.lt(columnName, value);
    }

    /**
     * generate "<=" statement value
     *
     * @param value column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> lte(Object value) {
        conditionSQL.append(" <= ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "<=" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <R>
     * @return BalalaQuery
     */
    public <S extends Model, R> BalalaQuery<T> lte(TypeFunction<S, R> function, Object value) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.lte(columnName, value);
    }

    /**
     * generate ">=" statement, simultaneous setting value
     *
     * @param column table column name [sql]
     * @param value  column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> gte(String column, Object value) {
        conditionSQL.append(" AND ").append(column).append(" >= ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "<" statement, simultaneous setting value
     *
     * @param column table column name [sql]
     * @param value  column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> lt(String column, Object value) {
        conditionSQL.append(" AND ").append(column).append(" < ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "<=" statement, simultaneous setting value
     *
     * @param column table column name [sql]
     * @param value  column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> lte(String column, Object value) {
        conditionSQL.append(" AND ").append(column).append(" <= ?");
        paramValues.add(value);
        return this;
    }

    /**
     * generate "in" statement, simultaneous setting value
     *
     * @param column table column name [sql]
     * @param args   column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> in(String column, Object... args) {
        if (null == args || args.length == 0) {
            log.warn("Column: {}, query params is empty.");
            return this;
        }
        conditionSQL.append(" AND ").append(column).append(" IN (");
        this.setArguments(args);
        conditionSQL.append(")");
        return this;
    }

    /**
     * generate "in" statement value
     *
     * @param args column value
     * @return BalalaQuery
     */
    public BalalaQuery<T> in(Object... args) {
        if (null == args || args.length == 0) {
            log.warn("Column: {}, query params is empty.");
            return this;
        }
        conditionSQL.append(" IN (");
        this.setArguments(args);
        conditionSQL.append(")");
        return this;
    }

    /**
     * Set in params
     *
     * @param list in param values
     * @param <S>
     * @return BalalaQuery
     */
    public <S> BalalaQuery<T> in(List<S> list) {
        return this.in(list.toArray());
    }

    /**
     * generate "in" statement, simultaneous setting value
     *
     * @param column column name
     * @param args   in param values
     * @param <S>
     * @return BalalaQuery
     */
    public <S> BalalaQuery<T> in(String column, List<S> args) {
        return this.in(column, args.toArray());
    }

    /**
     * generate "in" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param values   in param values
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> in(TypeFunction<T, R> function, Object... values) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.in(columnName, values);
    }

    /**
     * generate "in" statement with lambda, simultaneous setting value
     *
     * @param function column name with lambda
     * @param values   in param values
     * @param <R>
     * @return BalalaQuery
     */
    public <S, R> BalalaQuery<T> in(TypeFunction<T, R> function, List<S> values) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return this.in(columnName, values);
    }

    /**
     * generate order by statement
     *
     * @param order like "id desc"
     * @return BalalaQuery
     */
    public BalalaQuery<T> order(String order) {
        if (this.orderBySQL.length() > 0) {
            this.orderBySQL.append(',');
        }
        this.orderBySQL.append(' ').append(order);
        return this;
    }

    /**
     * generate order by statement
     *
     * @param columnName column name
     * @param orderBy    order by @see OrderBy
     * @return BalalaQuery
     */
    public BalalaQuery<T> order(String columnName, OrderBy orderBy) {
        if (this.orderBySQL.length() > 0) {
            this.orderBySQL.append(',');
        }
        this.orderBySQL.append(' ').append(columnName).append(' ').append(orderBy.toString());
        return this;
    }

    /**
     * generate order by statement with lambda
     *
     * @param function column name with lambda
     * @param orderBy  order by @see OrderBy
     * @param <R>
     * @return BalalaQuery
     */
    public <R> BalalaQuery<T> order(TypeFunction<T, R> function, OrderBy orderBy) {
        String columnName = AnimaUtils.getLambdaColumnName(function);
        return order(columnName, orderBy);
    }


    /**
     * query model by primary key
     *
     * @param id primary key value
     * @return model instance
     */
    public Future<JsonObject> byId(Object id) {
        this.beforeCheck();
        this.where(primaryKeyColumn, id);
        String sql   = this.buildSelectSQL(false);
        System.out.println(sql);
        Future<JsonObject> model = this.queryOne(sql, paramValues);
        return model;
    }

    /**
     * query models by primary keys
     *
     * @param ids primary key values
     * @return models
     */
    public Future<List<JsonObject>> byIds(Object... ids) {
        this.in(this.primaryKeyColumn, ids);
        return this.all();
    }

    /**
     * query and find one model
     *
     * @return one model
     */
    public Future<JsonObject> one() {
        this.beforeCheck();
        String sql   = this.buildSelectSQL(true);
        Future<JsonObject>      model = this.queryOne( sql, paramValues);
        return model;
    }

    /**
     * query and find all model
     *
     * @return model list
     */
   /* public Future<List<T>> all() {
        this.beforeCheck();
        String  sql    = this.buildSelectSQL(true);
        Future<List<T>> models = this.queryList(modelClass, sql, paramValues);
        return models;
    }*/

    public Future<List<JsonObject>> all() {
        this.beforeCheck();
        String  sql    = this.buildSelectSQL(true);
        Future<List<JsonObject>> models = this.queryList(sql, paramValues);
        return models;
    }




    /**
     * Querying a model
     *
     * @param type   model type
     * @param sql    sql statement
     * @param params params
     * @param <S>
     * @return S
     */
 /*   public <S> Future<S> queryOne(Class<S> type, String sql,JsonArray params) {
        Future<S> future = Future.future();
        SQLClient sqlClient = getSqlClient();
        sqlClient.queryWithParams(sql,params,res -> {
            System.out.println(sql);
            if (res.succeeded()){
                List<JsonObject> rows = res.result().getRows();
                if (rows!=null && rows.size()>0){
                    future.complete(rows.get(0).mapTo(type));
                }
            }else {
                future.fail(res.cause());
            }
        });
        this.clean();
        return future;
    }*/

    /**
     * Querying a model
     *
     * @param sql    sql statement
     * @param params params
     * @return S
     */
    public Future<JsonObject> queryOne(String sql, JsonArray params) {
        Future<JsonObject> future = Future.future();
        if (Balala.me().isUseSQLLimit()) {
            sql += " LIMIT 1";
        }
        SQLClient sqlClient = getSqlClient();

        sqlClient.queryWithParams(sql,params,res -> {
            if (res.succeeded()){
                List<JsonObject> rows = res.result().getRows();
                if (rows!=null && rows.size()>0){
                    future.complete(rows.get(0));
                }
            }else {
                future.fail(res.cause());
            }
            this.clean();
        });
        return future;
    }

    /**
     * Querying a list
     *
     * @param type   model type
     * @param sql    sql statement
     * @param params params
     * @param <S>
     * @return List<S>
     */
   /* public <S> Future<List<S>> queryList(Class<S> type, String sql, JsonArray params) {
        Future<List<S>> future = Future.future();
        SQLClient sqlClient = getSqlClient();
        sqlClient.queryWithParams(sql,params,res -> {
            if (res.succeeded()){
                List<JsonObject> rows = res.result().getRows();
                if (rows!=null && rows.size()>0){
                    List<S> result = new LinkedList<>();
                    rows.forEach(entries -> {result.add(entries.mapTo(type));});
                    future.complete(result);
                }
            }else {
                future.fail(res.cause());
            }
        });
        this.clean();
        return future;
    }*/

    /**
     * Querying a list
     *
     * @param sql    sql statement
     * @param params params
     * @param <S>
     * @return List<S>
     */
    public <S> Future<List<JsonObject>> queryList(String sql, JsonArray params) {
        Future<List<JsonObject>> future = Future.future();
        SQLClient sqlClient = getSqlClient();
        sqlClient.queryWithParams(sql,params,res -> {
            if (res.succeeded()){
                List<JsonObject> rows = res.result().getRows();
                if (rows!=null && rows.size()>0){
                    future.complete(rows);
                }
            }else {
                future.fail(res.cause());
            }
            this.clean();
        });
        return future;
    }


    /**
     * Paging query results
     *
     * @param page  page number
     * @param limit number each page
     * @return Page
     */
    public Future<Page<JsonObject>> page(int page, int limit) {
        return this.page(new PageRow(page, limit));
    }

    /**
     * Paging query results by sql
     *
     * @param sql     sql statement
     * @param pageRow page param
     * @return Page
     */
    public Future<Page<JsonObject>> page(String sql, PageRow pageRow) {
        return this.page(sql, paramValues, pageRow);
    }


    /**
     * Paging query results by sql
     *
     * @param sql     sql statement
     * @param params  param values
     * @param pageRow page param
     * @return Page
     */
    public Future<Page<JsonObject>> page(String sql, JsonArray params, PageRow pageRow) {

        Future<Page<JsonObject>> future = Future.future();
        this.beforeCheck();
        String     countSql = "SELECT COUNT(*) FROM (" + sql + ") tmp";
        SQLClient sqlClient = getSqlClient();

        //count
        sqlClient.queryWithParams(countSql,params, res -> {
            if (res.succeeded()){
                Long count = res.result().getResults().get(0).getLong(0);
                //page
                String  pageSQL = this.buildPageSQL(sql, pageRow);
                sqlClient.queryWithParams(pageSQL,params, pageRes ->{
                    if (pageRes.succeeded()){
                        List<JsonObject> rows = pageRes.result().getRows();
                        Page<JsonObject> pageBean = new Page<>(count, pageRow.getPageNum(), pageRow.getPageSize());
                        pageBean.setRows(rows);
                        future.complete(pageBean);
                    }else {
                        future.fail(res.cause());
                    }
                });
            }else{
                future.fail(res.cause());
            }
            this.clean();
        });
        return future;
    }

    /**
     * Paging query results
     *
     * @param pageRow page params
     * @return Page
     */
    public Future<Page<JsonObject>> page(PageRow pageRow) {
        String sql = this.buildSelectSQL(false);
        return this.page(sql, pageRow);
    }


   /* public Future<Long> count() {
        this.beforeCheck();
        String sql = this.buildCountSQL();
        return this.queryOne(Long.class, sql, paramValues);
    }*/

    /**
     * Count the number of rows.
     *
     * @return models count
     */
   public Future<Long> count(){
       Future<Long> future = Future.future();
       this.beforeCheck();
       String sql = this.buildCountSQL();
       SQLClient sqlClient = getSqlClient();
       sqlClient.queryWithParams(sql,paramValues, res -> {
            if (res.succeeded()){
                Long count = res.result().getResults().get(0).getLong(0);
                future.complete(count);
            }else {
                future.fail(res.cause());
            }
       });
       return future;
   }

    private void setArguments(Object[] args) {
        for (int i = 0; i < args.length; i++) {
            if (i == args.length - 1) {
                conditionSQL.append("?");
            } else {
                conditionSQL.append("?, ");
            }
            paramValues.add(args[i]);
        }
    }

    /**
     * Build a select statement.
     *
     * @param addOrderBy add the order by clause.
     * @return select sql
     */
    private String buildSelectSQL(boolean addOrderBy) {
        SQLParams sqlParams = SQLParams.builder()
            .modelClass(this.modelClass)
            .selectColumns(this.selectColumns)
            .tableName(this.tableName)
            .pkName(this.primaryKeyColumn)
            .conditionSQL(this.conditionSQL)
            .excludedColumns(this.excludedColumns)
            .isSQLLimit(isSQLLimit)
            .build();

        if (addOrderBy) {
            sqlParams.setOrderBy(this.orderBySQL.toString());
        }
        return Balala.me().getDialect().select(sqlParams);
    }

    /**
     * Build a count statement.
     *
     * @return count sql
     */
    private String buildCountSQL() {
        SQLParams sqlParams = SQLParams.builder()
            .modelClass(this.modelClass)
            .tableName(this.tableName)
            .pkName(this.primaryKeyColumn)
            .conditionSQL(this.conditionSQL)
            .build();
        return Balala.me().getDialect().count(sqlParams);
    }

    /**
     * Build a paging statement
     *
     * @param pageRow page param
     * @return paging sql
     */
    private String buildPageSQL(String sql, PageRow pageRow) {
        SQLParams sqlParams = SQLParams.builder()
            .modelClass(this.modelClass)
            .selectColumns(this.selectColumns)
            .tableName(this.tableName)
            .pkName(this.primaryKeyColumn)
            .conditionSQL(this.conditionSQL)
            .excludedColumns(this.excludedColumns)
            .customSQL(sql)
            .orderBy(this.orderBySQL.toString())
            .pageRow(pageRow)
            .build();
        return Balala.me().getDialect().paginate(sqlParams);
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
     * Clear the battlefield after a database operation.
     *
     */
    private void clean() {
        this.selectColumns = null;
        this.isSQLLimit = false;
        this.orderBySQL = new StringBuilder();
        this.conditionSQL = new StringBuilder();
        this.paramValues.clear();
        this.excludedColumns.clear();
    }
}

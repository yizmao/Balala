package io.github.balala;

import io.github.balala.core.BalalaUpdate;
import io.github.balala.core.ResultKey;
import io.github.balala.core.functions.TypeFunction;
import io.vertx.core.Future;

import java.io.Serializable;

/**
 * Base Model
 * @author yizmao
 * @date 18-6-11 上午11:18
 */
public class Model {

    /**
     * The update object for the current model.
     */
    private BalalaUpdate<? extends Model> update = new BalalaUpdate<>(this.getClass());

    /**
     * insert
     * @return Primary key
     */
    public Future<ResultKey> save(){
        return this.update.save(this);
    }

    /**
     * update
     * @return number of rows affected after execution
     */
    public Future<Integer> update(){
        return this.update.updateByModel(this);
    }

    /**
     * update by Primary key
     * @return number of rows affected after execution
     */
    public Future<Integer> updateById(Serializable id){
        return new BalalaUpdate<>(this.getClass()).updateById(this, id);
    }
    /**
     * delete
     *
     * @return number of rows affected after execution
     */
    public Future<Integer> delete(){
        return this.update.deleteByModel(this);
    }

    /**
     * Update set statement
     *
     * @param column table column name [sql]
     * @param value  column value
     * @return AnimaQuery
     */
    public BalalaUpdate<? extends Model> set(String column, Object value) {
        return update.set(column, value);
    }

    /**
     * Update set statement with lambda
     *
     * @param function table column name with lambda
     * @param value    column value
     * @param <T>
     * @param <R>
     * @return AnimaQuery
     */
    public <T extends Model, R> BalalaUpdate<? extends Model> set(TypeFunction<T, R> function, Object value) {
        return update.set(function, value);
    }

    /**
     * Where statement
     *
     * @param statement conditional clause
     * @param value     column value
     * @return AnimaQuery
     */
    public BalalaUpdate<? extends Model> where(String statement, Object value) {
        return update.where(statement, value);
    }

    /**
     * Where statement with lambda
     *
     * @param function column name with lambda
     * @param value    column value
     * @param <T>
     * @param <R>
     * @return AnimaQuery
     */
    public <T extends Model, R> BalalaUpdate<? extends Model> where(TypeFunction<T, R> function, Object value) {
        return update.where(function, value);
    }
}

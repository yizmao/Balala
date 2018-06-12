/**
 * Copyright (c) 2018, biezhi 王爵 (biezhi.me@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.balala.core;


import io.github.balala.Model;
import io.github.balala.page.Page;
import io.github.balala.page.PageRow;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

/**
 * ResultList
 * <p>
 * Get a list of collections or single data
 *
 * @author biezhi
 * @date 2018/3/16
 */
public class ResultList<T> {


    private final Class<T> type;
    private final String   sql;
    private final JsonArray params;

    public ResultList(Class<T> type, String sql, JsonArray params) {
        this.type = type;
        this.sql = sql;
        this.params = params;
    }

    public Future<JsonObject> one() {
        return new BalalaQuery<>().queryOne(sql, params);
    }

    public Future<List<JsonObject>> all() {
        return new BalalaQuery<>().queryList(sql, params);
    }

    public <S extends Model> Future<Page<JsonObject>> page(PageRow pageRow) {
        Class<S> modelType = (Class<S>) type;
        return new BalalaQuery<>(modelType).page(sql, params, pageRow);
    }

    public <S extends Model> Future<Page<JsonObject>> page(int page, int limit) {
        return this.page(new PageRow(page, limit));
    }

}

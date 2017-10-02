/*******************************************************************************
 * Copyright 2017 Kurt Bliefernich
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.curtkurt.mongo

import com.mongodb.BasicDBList
import com.mongodb.BasicDBObject


/**
 * Abstraction of BasicDBList used in mongo aggregations
 */
abstract class ListBuilder implements MongoBuilder{
    protected BasicDBList list
    protected QueryPipeBuilder builder

    /**
     * Creates the {@see BasicDBList} and associates the QueryPipeBuilder
     * @param builder
     */
    ListBuilder(QueryPipeBuilder builder) {
        this.builder = builder
        this.list = new BasicDBList()
    }

    /**
     * Add a field value to a ListBuilder type
     * @param field to match
     * @param value to match
     * @return extends ListBuilder
     */
    ListBuilder addOption(String field, String value) {
        list << new BasicDBObject(field,value)
        return this
    }

    /**
     * Adds field value match combinations to a ListBuilder type
     * @param options of field/value combinations
     * @return extends ListBuilder
     */
    ListBuilder addOptions(Map<String,Object> options) {
        options.each {k,v ->
            list << new BasicDBObject(k,v)
        }
        return this
    }

    /**
     * Returns focus of the builder to the {@see QueryPipeBuilder}
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder end() {
        return builder
    }
}

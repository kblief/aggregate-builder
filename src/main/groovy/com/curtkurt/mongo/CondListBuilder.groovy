/*******************************************************************************
 * Copyright 2018 Kurt Bliefernich
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

/**
 * Abstraction of BasicDBList used in mongo aggregations
 */
abstract class CondListBuilder implements MongoBuilder {

    protected BasicDBList list
    protected QueryPipeBuilder builder
    protected MongoBuilder parent

    /**
     * Creates the {@see BasicDBList} and associates the QueryPipeBuilder
     * @param builder
     */
    CondListBuilder(QueryPipeBuilder builder, MongoBuilder parent) {
        this.builder = builder
        this.parent = parent
        this.list = new BasicDBList()
    }


    /**
     * Returns focus of the builder to the {@see QueryPipeBuilder}
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder end() {
        return builder
    }

    /**
     * Returns focus of the builder to the parent {@see MongoBuilder}
     * @return MongoBuilder
     */
    MongoBuilder close() {
        return parent
    }
}

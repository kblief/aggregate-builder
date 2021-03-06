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

import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import groovy.transform.ToString


/**
 * Adds a $filter to query pipeline
 */
@ToString(includeFields = true, excludes = ['builder', 'metaClass', 'parent'], includePackage = false)
class Filter implements MongoBuilder {
    private DBObject dbObject
    private QueryPipeBuilder builder
    private MongoBuilder parent

    Filter(QueryPipeBuilder builder, String field, MongoBuilder parent, String asName = "${field}_") {
        dbObject = new BasicDBObject('$filter', new BasicDBObject([input: "\$$field", as: "$asName"]))
        this.builder = builder
        this.parent = parent
    }

    Condition cond() {
        def condition = new Condition(builder,parent)
        dbObject.$filter.cond = condition
        return condition
    }


    @Override
    def build() {
        if (!dbObject.$filter.cond) {
            throw new IllegalStateException('Filter requires an array to operate on with a condition')
        }

        if (dbObject.$filter.cond instanceof MongoBuilder) {
            def cond = dbObject.$filter.remove('cond')
            dbObject.$filter << cond.build()
        }

        dbObject
    }

}

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
import com.mongodb.DBObject

/**
 * Represents an $and in mongo aggregate.
 * Exposes methods that can be used to build an $and
 *
 * @see {ListBuilder}
 */
class AndList extends ListBuilder {
    private DBObject dbObject

    AndList(QueryPipeBuilder builder) {
        super(builder)
        this.dbObject = new BasicDBObject('$and',list)
    }

    /**
     * Adds a $gt on field with a value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$gt:value&#125;&#125;]&#125;
     *   ... .and().gt(field,value)
     * }
     * </pre>
     * @param field to perform gt on
     * @param value to be gt
     * @return AndList
     */
    AndList gt(String field, value) {
        list << new BasicDBObject(field, new BasicDBObject('$gt', value))
        return this
    }

    /**
     * Adds a $gte on field with a  value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$gte:value&#125;&#125;]&#125;
     *   ... .and().gte(field,value)
     * }
     * </pre>
     * @param field to perform gte on
     * @param value to be gte to
     * @return AndList
     */
    AndList gte(String field,value) {
        list << new BasicDBObject(field, new BasicDBObject('$gte', value))
        return this
    }

    /**
     * Adds a $lt on a field with a value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$lt:value&#125;&#125;]&#125;
     *   ... .and().lt(field,value)
     * }
     * </pre>
     * @param field to perform a lt on
     * @param value to be lt
     * @return AndList
     */
    AndList lt(String field, value) {
        list << new BasicDBObject(field, new BasicDBObject('$lt', value))
        return this
    }

    /**
     * Adds a $lte on a field with a value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$lte:value&#125;&#125;]&#125;
     *   ... .and().lte(field,value)
     * }
     * </pre>
     * @param field to perform a lte on
     * @param value to be lte
     * @return AndList
     */
    AndList lte(String field,value) {
        list << new BasicDBObject(field, new BasicDBObject('$lte', value))
        return this
    }

    def build() {
        def cleanedList = new BasicDBList()
        list.each {
            if (it instanceof MongoBuilder ) {
                cleanedList << it.build()
            } else {
                cleanedList << it
            }
        }
        dbObject.$and = cleanedList
        return dbObject
    }
}

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

@ToString(includeFields = true, excludes = ['builder', 'metaClass', 'parent'])
class Size implements MongoBuilder {

    private DBObject dbObject
    private QueryPipeBuilder builder
    private MongoBuilder parent

    Size(QueryPipeBuilder builder, MongoBuilder parent) {
        this.builder = builder
        this.parent = parent
    }

    /**
     *
     * @param arrayField to filter
     * @param options
     * @return
     */
    Filter filter(String field, String asName = null) {
        def filter = new Filter(builder, field, parent, asName)
        dbObject = new BasicDBObject('$size', filter)
        return filter
    }

    MongoBuilder size(String field) {
        dbObject = new BasicDBObject('$size', "\$$field")
        return parent
    }

    /**
     * Builds the Size object; builds and {@link MongoBuilder} types in the $size
     * @return DBObject
     */
    @Override
    DBObject build() {
        if (!dbObject || !dbObject.$size ) {
            throw new IllegalStateException('Size is not defined')
        }
        dbObject.each { k,v ->
            if ( v instanceof MongoBuilder ) {
                dbObject[k] = v.build()
            }
        }
        return dbObject
    }
}

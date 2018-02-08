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
 * Adds a $project to query pipeline
 */
@ToString(includeFields = true, excludes = ['builder', 'metaClass'])
class Projection implements MongoBuilder {

    private DBObject dbObject
    private QueryPipeBuilder builder

    Projection(QueryPipeBuilder builder) {
        this.builder = builder
    }

    /**
     * Creates a $project of fields in collection
     * <pre>
     * {@code
     *   // &#123;$project:&#123;field:displayValue&#125;&#125;
     *   builder.projection().project(fields)
     *}
     * </pre>
     *
     * The fields map is fieldname to 1 or 0.
     * A 1 is to show the field, a 0 is to hide a field.
     * A field not included is hidden with the exception of the
     * _id field.  The _id field has to be explicitly hidden with a 0
     *
     * @param fields to project from collection
     * @return QueryPipeBuilder
     */
    Projection project(Map<String, Integer> fields) {
        dbObject = new BasicDBObject('$project', fields)
        return this
    }

    /**
     *
     * @param fieldName
     * @param field
     * @return
     */
    Projection size(String fieldName, String field) {
        if (dbObject?.$project) {
            dbObject.$project << new BasicDBObject(fieldName, new BasicDBObject('$size', "\$$field"))
        } else {
            dbObject = new BasicDBObject('$project', new BasicDBObject(fieldName, new BasicDBObject('$size', "\$$field")))
        }

        return this
    }

    /**
     *
     * @param fieldName
     * @param field
     * @return
     */
    Size size(String fieldName) {
        Size size = new Size(builder, this)
        if (dbObject && dbObject.$project) {
            dbObject.$project << new BasicDBObject(fieldName, size)
        } else {
            dbObject = new BasicDBObject('$project', new BasicDBObject(fieldName, size))
        }

        return size
    }

    /**
     *
     * @param arrayField to filter
     * @param options
     * @return
     */
    Filter filter(String field, String asField = null) {
        def filter = new Filter(builder, field, this, asField)
        def filterField = new BasicDBObject(field, filter)
        if (dbObject && dbObject.$project) {
            dbObject.$project << filterField
        } else {
            dbObject = new BasicDBObject('$project', filterField)
        }
        return filter
    }

    QueryPipeBuilder end() {
        return builder
    }

    /**
     * Builds the Projection object; builds and {@link MongoBuilder} types in the $project
     * @return DBObject
     */
    @Override
    DBObject build() {
        if (!dbObject || !dbObject.$project) {
            throw new IllegalStateException('Projection is not defined')
        }
        dbObject.$project.each { k, v ->
            if (v instanceof MongoBuilder) {
                dbObject.$project[k] = v.build()
            }
        }
        return dbObject
    }
}

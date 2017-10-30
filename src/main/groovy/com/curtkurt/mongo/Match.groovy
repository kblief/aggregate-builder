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
import groovy.transform.ToString

/**
 * Collection of $match aggregate operations
 */
@ToString(excludes = ['builder','metaClass'], includeFields = true, includePackage = false)
class Match implements MongoBuilder {
    private DBObject dbObject
    private QueryPipeBuilder builder

    Match(QueryPipeBuilder builder) {
        this.builder = builder
    }

    /**
     * Creates a $match of field to value
     * <pre>
     * {@code
     *   // &#123;$match:&#123;field:value&#125;&#125;
     *   builder.match().equal(field,value)
     * }
     * </pre>
     * @param field to match
     * @param value value to match
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder equal(String field, value) {
        if ( value ) {
            dbObject = new BasicDBObject('$match', new BasicDBObject(field, value))
        }
        return builder
    }

    /**
     * Creates a $match on 'null'
     * <pre>
     * {@code
     *   //&#123;$match:&#123;field:null&#125;&#125;
     *   builder.match().isNull(field)
     * }
     * </pre>
     * @param field to not match on
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder isNull(String field) {
        dbObject = new BasicDBObject('$match', new BasicDBObject(field, null))
        return builder
    }

    /**
     * Adds a $match of the field to null
     *
     * <pre>
     * {@code
     *   // &#123;$match: &#123;field:&#123;$exists:true&#125;&#125;&#125;
     *   builder.matchExists(field)
     * }
     * </pre>
     * @param field to match exists
     * @param boolean exists true by default
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder exists(String field, boolean exists = true) {
        dbObject = new BasicDBObject('$match', new BasicDBObject(field,new BasicDBObject('$exists',exists)))
        return builder
    }


    /**
     * Creates a $match on a field to a list of values using an $in operator
     * <pre>
     * {@code
     *   // &#123;$match:&#123;field:&#123;$in:['a','b']&#125;&#125;&#125;
     *   builder.match().inList(field,['a','b'])
     * }
     * </pre>
     * @param field to match
     * @param values to match
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder inList(String field, values) {
        dbObject = new BasicDBObject('$match', new BasicDBObject(field, new BasicDBObject('$in', values)))
        return builder
    }

    /**
     * Creates a $match to between a range exclusive
     * <pre>
     * {@code
     *   //&#123;$match:&#123;$and:[&#123;field:&#123;$gt:10&#125;&#125;,&#123;field:&#123;$lt:100&#125;&#125;]&#125;&#125;
     *   builder.match().betweenRange(field,new IntegerRange(100,10)
     * }
     * </pre>
     * @param field to match between
     * @param range to match between
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder betweenRange(String field, Range range) {
        DBObject andList = new BasicDBList()
        andList.addAll([new BasicDBObject(field, new BasicDBObject('$gt', range.lower)),
                        new BasicDBObject(field, new BasicDBObject('$lt', range.upper))])
        dbObject = new BasicDBObject('$match', new BasicDBObject('$and', andList))
        return builder
    }

    /**
     * Creates a $match between a range inclusive of the lower bound
     *  <pre>
     * {@code
     *   //&#123;$match:&#123;$and:[&#123;field:&#123;$gte:10&#125;&#125;,&#123;field:&#123;$lt:100&#125;&#125;]&#125;&#125;
     *   builder.match().betweenRangeLowerInclusive(field,new IntegerRange(100,10)
     * }
     * </pre>
     * @param field to match between
     * @param range to match
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder betweenRangeLowerInclusive(String field, Range range) {
        DBObject andList = new BasicDBList()
        andList.addAll([new BasicDBObject(field, new BasicDBObject('$gte', range.lower)),
                        new BasicDBObject(field, new BasicDBObject('$lt', range.upper))])
        dbObject = new BasicDBObject('$match', new BasicDBObject('$and', andList))
        return builder
    }

    /**
     * Creates a $match between a range of the upper bound inclusive
     *  <pre>
     * {@code
     *   //&#123;$match:&#123;$and:[&#123;field:&#123;$gt:10&#125;&#125;,&#123;field:&#123;$lte:100&#125;&#125;]&#125;&#125;
     *   builder.match().betweenRangeUpperInclusive(field,new IntegerRange(100,10)
     * }
     *  </pre>
     * @param field
     * @param range
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder betweenRangeUpperInclusive(String field, Range range) {
        DBObject andList = new BasicDBList()
        andList.addAll([new BasicDBObject(field, new BasicDBObject('$gt', range.lower)),
                        new BasicDBObject(field, new BasicDBObject('$lte', range.upper))])
        dbObject = new BasicDBObject('$match', new BasicDBObject('$and', andList))
        return builder
    }

    /**
     * Creates a $match between a range inclusive of the upper and lower bounds
     * <pre>
     * {@code
     *   //&#123;$match:&#123;$and:[&#123;field:&#123;$gte:10&#125;&#125;,&#123;field:&#123;$lte:100&#125;&#125;]&#125;&#125;
     *   builder.match().betweenRangeInclusive(field,new IntegerRange(100,10)
     * }
     *  </pre>
     * @param field to match between
     * @param range to match between
     * @return QueryPipeBuilder
     */
    QueryPipeBuilder betweenRangeInclusive(String field, Range range) {
        DBObject andList = new BasicDBList()
        andList.addAll([new BasicDBObject(field, new BasicDBObject('$gte', range.lower)),
                        new BasicDBObject(field, new BasicDBObject('$lte', range.upper))])
        dbObject = new BasicDBObject('$match', new BasicDBObject('$and', andList))
        return builder
    }

    /**
     * Adds a $or to a $match; exposes the {@see OrList}
     * <pre>
     * {@code
     *   // &#123;$match&#123;$or[]&#125;&#125;
     *   builder.match().or()
     * }
     * </pre>
     * @return OrList
     */
    OrList or() {
        def orList = new OrList(builder)
        dbObject = new BasicDBObject('$match', orList)
        return orList
    }

    /**
     * Adds an $and to a $match; exposes the AndList 
     * <pre>
     * {@code
     *   // &#123;$match: &#123;$and:[]&#125;&#125;
     *   builder.match().and()
     * }
     * </pre>
     * @see AndList
     * @return AndList
     */
    AndList and() {
        def andList = new AndList(builder)
        dbObject = new BasicDBObject('$match', andList)
        return andList
    }

    /**
     * Adds an $and to a $match; exposes the Not                                        List
     * <pre>
     * {@code
     *   // &#123;$match: &#123;$not:[]&#125;&#125;
     *   builder.match().not()
     * }
     * </pre>
     * @see AndList
     * @return AndList
     */
    NotList not() {
        def notList = new NotList(builder)
        dbObject = new BasicDBObject('$match', notList)
        return notList
    }

    /**
     * Builds the match object; builds and {@link MongoBuilder} types in the $match
     * @return DBObject
     */
    DBObject build() {
        if (!dbObject) {
            throw new IllegalStateException('Match is not defined')
        }
        if (dbObject.$match instanceof MongoBuilder) {
            dbObject.$match = dbObject.$match.build()
        }
        return dbObject
    }
}

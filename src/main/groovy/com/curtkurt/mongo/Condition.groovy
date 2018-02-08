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
import com.mongodb.BasicDBObject
import com.mongodb.DBObject
import groovy.transform.ToString

@ToString(includeFields = true, excludes = ['builder', 'metaClass', 'parent'])
class Condition implements MongoBuilder {

    private DBObject dbObject
    private QueryPipeBuilder builder
    private MongoBuilder parent

    Condition(QueryPipeBuilder builder, MongoBuilder parent) {
        this.builder = builder
        this.parent = parent
    }

    /**
     * Adds an equal on a field and value to an AndList
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:value&#125;]&#125;
     *   ... .or().equal(field,value)
     *}
     * </pre>
     * @param field to equal
     * @param value to match
     * @return OrList
     */
    QueryPipeBuilder eq(String field, value) {
        def eqList = new BasicDBList()
        eqList << '$$' + field
        eqList << value
        dbObject = new BasicDBObject('cond',new BasicDBObject('$eq', eqList))
        return builder
    }

    /**
     * Adds a $gt on field with a value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$gt:value&#125;&#125;]&#125;
     *   ... .and().gt(field,value)
     *}
     * </pre>
     * @param field to perform gt on
     * @param value to be gt
     * @return AndList
     */
    QueryPipeBuilder gt(String field, value) {
        def gtList = new BasicDBList()
        gtList << '$$' + field
        gtList << value
        dbObject = new BasicDBObject('cond',new BasicDBObject('$gt', gtList))
        return builder
    }

    /**
     * Adds a $gte on field with a  value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$gte:value&#125;&#125;]&#125;
     *   ... .and().gte(field,value)
     *}
     * </pre>
     * @param field to perform gte on
     * @param value to be gte to
     * @return AndList
     */
    QueryPipeBuilder gte(String field, value) {
        def gteList = new BasicDBList()
        gteList << '$$' + field
        gteList << value
        dbObject =  new BasicDBObject('cond',new BasicDBObject('$gte', gteList))
        return builder
    }

    /**
     * Adds a $lt on a field with a value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$lt:value&#125;&#125;]&#125;
     *   ... .and().lt(field,value)
     *}
     * </pre>
     * @param field to perform a lt on
     * @param value to be lt
     * @return AndList
     */
    QueryPipeBuilder lte(String field, value) {
        def ltList = new BasicDBList()
        ltList << '$$' + field
        ltList << value
        dbObject =  new BasicDBObject('cond',new BasicDBObject('$lte', ltList))
        return builder
    }

    /**
     * Adds a $lt on a field with a value
     * <pre>
     * {@code
     *   // ...&#123;$and:[&#123;field:&#123;$lt:value&#125;&#125;]&#125;
     *   ... .and().lt(field,value)
     *}
     * </pre>
     * @param field to perform a lt on
     * @param value to be lt
     * @return AndList
     */
    QueryPipeBuilder lt(String field, value) {
        def ltList = new BasicDBList()
        ltList << '$$' + field
        ltList << value
        dbObject =  new BasicDBObject('cond',new BasicDBObject('$lt', ltList))
        return builder
    }

    /**
     * Adds a $or to a $match; exposes the {@see OrList}
     * <pre>
     * {@code
     *   // &#123;cond&#123;$or[]&#125;&#125;
     *   builder.match().or()
     *}
     * </pre>
     * @return OrList
     */
    CondOrList or() {
        def orList = new CondOrList(builder,parent)
        dbObject = new BasicDBObject('cond', orList)
        return orList
    }

    /**
     * Adds an $and to a $match; exposes the AndList
     * <pre>
     * {@code
     *   // &#123;cond: &#123;$and:[]&#125;&#125;
     *   builder.match().and()
     *}
     * </pre>
     * @see AndList
     * @return AndList
     */
    CondAndList and() {
        def andList = new CondAndList(builder,parent)
        dbObject = new BasicDBObject('cond', andList)
        return andList
    }

    /**
     * Builds the Condition object; builds and {@link MongoBuilder} types in the cond in filter
     * @return DBObject
     */
    @Override
    DBObject build() {
        if (!dbObject) {
            throw new IllegalStateException('Condition is not defined')
        }
        if (dbObject.cond instanceof MongoBuilder) {
            dbObject.cond = dbObject.cond.build()
        }
        return dbObject
    }
}
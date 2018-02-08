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

@ToString(includeFields = true, excludes = ['builder', 'metaClass', 'parent'], includePackage = false)
class CondOrList extends CondListBuilder {
    private DBObject dbObject

    CondOrList(QueryPipeBuilder builder, MongoBuilder parent) {
        super(builder, parent)
        this.dbObject = new BasicDBObject('$or', list)
    }

    /**
     * Adds an equal on a field and value to an OrList
     * <pre>
     * {@code
     *   // ...&#123;$or:[&#123;field:value&#125;]&#125;
     *   ... .or().equal(field,value)
     *}
     * </pre>
     * @param field to equal
     * @param value to match
     * @return OrList
     */
    CondOrList eq(String field, value) {
        def eqList = new BasicDBList()
        eqList << '$$' + field
        eqList << value
        list << new BasicDBObject('$eq', eqList)
        return this
    }

    /**
     * Adds a $gt on field with a value
     * <pre>
     * {@code
     *   // ...&#123;$or:[&#123;field:&#123;$gt:value&#125;&#125;]&#125;
     *   ... .and().gt(field,value)
     *}
     * </pre>
     * @param field to perform gt on
     * @param value to be gt
     * @return AndList
     */
    CondOrList gt(String field, value) {
        def gtList = new BasicDBList()
        gtList << '$$' + field
        gtList << value
        list << new BasicDBObject('$gt', gtList)
        return this
    }

    /**
     * Adds a $gte on field with a  value
     * <pre>
     * {@code
     *   // ...&#123;$or:[&#123;field:&#123;$gte:value&#125;&#125;]&#125;
     *   ... .and().gte(field,value)
     *}
     * </pre>
     * @param field to perform gte on
     * @param value to be gte to
     * @return AndList
     */
    CondOrList gte(String field, value) {
        def gteList = new BasicDBList()
        gteList << '$$' + field
        gteList << value
        list << new BasicDBObject('$gte', gteList)
        return this
    }

    /**
     * Adds a $lt on a field with a value
     * <pre>
     * {@code
     *   // ...&#123;$or:[&#123;field:&#123;$lt:value&#125;&#125;]&#125;
     *   ... .and().lt(field,value)
     *}
     * </pre>
     * @param field to perform a lt on
     * @param value to be lt
     * @return AndList
     */
    CondOrList lt(String field, value) {
        def ltList = new BasicDBList()
        ltList << '$$' + field
        ltList << value
        list << new BasicDBObject('$lt', ltList)
        return this
    }

    /**
     * Adds a $lte on a field with a value
     * <pre>
     * {@code
     *   // ...&#123;$or:[&#123;field:&#123;$lte:value&#125;&#125;]&#125;
     *   ... .and().lte(field,value)
     *}
     * </pre>
     * @param field to perform a lte on
     * @param value to be lte
     * @return AndList
     */
    CondOrList lte(String field, value) {
        def lteList = new BasicDBList()
        lteList << '$$' + field
        lteList << value
        list << new BasicDBObject('$lte', lteList)
        return this
    }

    /**
     * Adds an $and to a $or; exposes the CondAndList
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
        def andList = new CondAndList(builder, parent)
        list.add(andList)
        return andList
    }

    def build() {
        def cleanedList = new BasicDBList()
        list.each {
            if (it instanceof MongoBuilder) {
                cleanedList << it.build()
            } else {
                cleanedList << it
            }
        }
        dbObject.$or = cleanedList
        return dbObject
    }
}

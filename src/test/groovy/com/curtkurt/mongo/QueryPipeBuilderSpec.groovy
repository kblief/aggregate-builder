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
import spock.lang.Specification
import spock.lang.Subject
import spock.lang.Unroll

import static com.curtkurt.mongo.QueryPipeBuilder.Sort.ASC
import static com.curtkurt.mongo.QueryPipeBuilder.Sort.DESC

class QueryPipeBuilderSpec extends Specification {

    @Subject
    QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should add match to queryPipe"() {
        given: "I have a field and value"
        String field = "a"
        String value = 'value'

        when: "I build a queryPipe with a match on the field and value"
        builder.match(field, value)
        def queryPipe = builder.build()

        then: "the querypipe contains a match on that field and value"
        queryPipe[0].$match.getAt(field) == value
    }

    @Unroll
    void "should not add match to queryPipe when value is falsey"() {
        given: "I have a field and value"
        String field = "a"
        String value = fieldValue

        when: "I build a queryPipe with a match on the field and value"
        builder.match(field, value)
        def queryPipe = builder.build()

        then: "the querypipe contains a match on that field and value"
        queryPipe[0]?.$match?.getAt(field) == outcome

        where:
        fieldValue | outcome
        null       | null
        ""         | ""
    }

    void "should add match null to querypipe"() {
        given: "I have a field"
        def field = 'field'

        when: "I build the query pipe with a match null"
        builder.matchNull(field)
        def queryPipe = builder.build()

        then: "querypipe has a null match on the field"
        queryPipe[0].$match.getAt(field) == null
    }

    void "should match field to values in list"() {
        given: "I have a field and a list of values"
        def field = 'field'
        def listOfValues = ['a', 'b', 'c']

        when: "I build a querypipe to match field to list of values"
        builder.matchInList(field, listOfValues)
        def queryPipe = builder.build()

        then: "the querypipe contains a match on field to list of values"
        queryPipe[0].$match.getAt(field).$in == listOfValues
    }

    void "should create or match of field to values"() {
        given: "I have a field and a list of values"
        def field = 'field'
        def values = ['a', 'b', 'c']

        and: "the expected results are"
        def expected = [new BasicDBObject(field, values[0]),
                        new BasicDBObject(field, values[1]),
                        new BasicDBObject(field, values[2])]

        when: "I build the queryPipe to match or in list"
        builder.matchOrList(field, values)
        def queryPipe = builder.build()

        then: "the query pipe contains a match or of the list of values"
        queryPipe[0].$match.$or == expected
    }

    void "should add unwind to querypipe"() {
        given: "I have a field"
        def field = 'field'

        when: "I build a query pipe with an unwind"
        def queryPipe = builder.unwindField(field).build()

        then: "the querypipe contains an unwind on the field"
        queryPipe[0].$unwind == [path: '$' + field, preserveNullAndEmptyArrays: false]
    }

    void "should add unwind to querypipe and preserve null and empty"() {
        given: "I have a field"
        def field = 'field'

        when: "I build a query pipe with an unwind"
        def queryPipe = builder.unwindField(field, [outerJoin: true]).build()

        then: "the querypipe contains an unwind on the field"
        queryPipe[0].$unwind == [path: '$' + field, preserveNullAndEmptyArrays: true]
    }

    void "should add a basic join on a field"() {
        given: "I have a field I want to join on"
        def field = 'field'

        when: "I add a basic join"
        def queryPipe = builder.addBasicJoin(field).build()

        then: "the querypipe should have a join and an unwind"
        queryPipe[0].$lookup.from == field
        queryPipe[0].$lookup.localField == field
        queryPipe[0].$lookup.foreignField == '_id'
        queryPipe[0].$lookup.as == field

        and:
        queryPipe[1].$unwind == [path: '$' + field, preserveNullAndEmptyArrays: false]
    }

    void "should add defined join to querypipe"() {
        given: "I have a field and a defined join"
        def field = 'field'
        def fieldConfig = [from        : 'collection',
                           localField  : 'localField',
                           foreignField: 'foreignField',
                           as          : 'as',
                           outerJoin   : true]

        when: "I add a defined join"
        def queryPipe = builder.addJoin(field, fieldConfig).build()

        then: "the querypipe contains the lookup"
        queryPipe[0].$lookup.from == fieldConfig.from
        queryPipe[0].$lookup.localField == fieldConfig.localField
        queryPipe[0].$lookup.foreignField == fieldConfig.foreignField
        queryPipe[0].$lookup.as == fieldConfig.as

        and:
        queryPipe[1].$unwind == [path                      : '$' + fieldConfig.as,
                                 preserveNullAndEmptyArrays: fieldConfig.outerJoin]

    }

    void "should add defined join to querypipe without unwind"() {
        given: "I have a field and a defined join"
        def field = 'field'
        def fieldConfig = [from        : 'collection',
                           localField  : 'localField',
                           foreignField: 'foreignField',
                           as          : 'as',
                           outerJoin   : true]

        when: "I add a defined join"
        def queryPipe = builder.lookup(field, fieldConfig).build()

        then: "the querypipe contains the lookup"
        queryPipe[0].$lookup.from == fieldConfig.from
        queryPipe[0].$lookup.localField == fieldConfig.localField
        queryPipe[0].$lookup.foreignField == fieldConfig.foreignField
        queryPipe[0].$lookup.as == fieldConfig.as
    }

    void "should add asc sort order to the querypipe"() {
        given: "I have a field and an asc order"
        def field = 'field'
        def asc = ASC

        when: "I build add sort order and build the querypipe"
        def queryPipe = builder.addSortOrder(field, asc).build()

        then: "sort order should be in the query pipe"
        queryPipe[0].$sort.getAt(field) == ASC.value
    }

    void "should add desc sort order to the querypipe"() {
        given: "I have a field and an asc order"
        def field = 'field'
        def desc = DESC

        when: "I build add sort order and build the querypipe"
        def queryPipe = builder.addSortOrder(field, desc).build()

        then: "sort order should be in the query pipe"
        queryPipe[0].$sort.getAt(field) == DESC.value
    }

    void "should add default sort order to the querypipe"() {
        given: "I have a field and an asc order"
        def field = 'field'

        when: "I build add sort order and build the querypipe"
        def queryPipe = builder.addSortOrder(field).build()

        then: "sort order should be in the query pipe"
        queryPipe[0].$sort.getAt(field) == ASC.value
    }

    void "should not add sort if the field is null"() {
        given: "I have a null field"
        def field = null

        when: "I add sort"
        def pipe = builder.addSortOrder(field, QueryPipeBuilder.Sort.ASC).build()

        then: "the pipeline does not have a sort"
        pipe.isEmpty()
    }

    void "should add pagination to the querypipe"() {
        when: "I add pagination"
        def queryPipe = builder.addPagination(0).build()

        then: "skip is added to the queryPipe"
        queryPipe[0].$skip == QueryPipeBuilder.DEFAULT_OFFSET

        and: "limit is added to the querypipe"
        queryPipe[1].$limit == QueryPipeBuilder.DEFAULT_MAX
    }

    void "should add defined pagination to the querypipe"() {
        given: "I have a defined max"
        def max = 100

        when: "I add pagination"
        def queryPipe = builder.addPagination(max, 0).build()

        then: "skip is added to the queryPipe"
        queryPipe[0].$skip == QueryPipeBuilder.DEFAULT_OFFSET

        and: "limit is added to the querypipe"
        queryPipe[1].$limit == max
    }

    void "should add offset defined pagination to the querypipe"() {
        given: "I have a defined max"
        def offset = 100

        when: "I add pagination"
        def queryPipe = builder.addPagination(QueryPipeBuilder.DEFAULT_MAX, offset).build()

        then: "skip is added to the queryPipe"
        queryPipe[0].$skip == offset

        and: "limit is added to the querypipe"
        queryPipe[1].$limit == QueryPipeBuilder.DEFAULT_MAX
    }

    void "should add custom dbObject"() {
        given: "I have a custom DBObject"
        def field = 'field'
        def thing = new BasicDBObject('$match', field)

        when: "I add the custom object"
        def pipe = builder.addCustom(thing).build()

        then: "the pipe has the custom object"
        pipe[0].$match == field

    }

    void "should say whether the pipe is populated"() {
        given: "I have a query pipe with at least one entry"
        builder.match('field', 'value')

        when: "I check to see if the pipe is populated"
        def isPopulated = builder.isPopulated()

        then:
        isPopulated
    }

    void "should say pipe is not populated"() {
        expect: "An empty builder has no pipe"
        !builder.isPopulated()
    }

    void "should get an instance of Match when match is called"() {
        when: "I get a match"
        def match = builder.match()

        then: "the result is an instance of Match"
        match instanceof Match
    }

    void "should create new instance of QueryPipeBuilder when copy is called"() {
        given: "I have a query pipe builder and I add conditions"
        builder.match()
                .equal('field', 'value')
                .addPagination(10, 0)
                .addSortOrder('field', ASC)

        when: "I create a copy"
        def copy = QueryPipeBuilder.copy(builder)

        then: "copy and builder have the same queryPipe"
        copy.build() == builder.build()

    }

    void "should create new instance of QueryPipeBuilder when copy is called and not have the same query pipe"() {
        given: "I have a query pipe builder and I add conditions"
        builder.match()
                .equal('field', 'value')
                .addPagination(10, 0)
                .addSortOrder('field', ASC)

        when: "I create a copy"
        def copy = QueryPipeBuilder.copy(builder)

        and: "I make changes to the copy"
        copy.matchNull('otherField')

        then: "copy and builder do NOT have the same queryPipe"
        copy.build() != builder.build()
    }

    void "should add group count"() {
        given: "I have a field"
        def field = 'field'

        when: "I add a group count"
        def pipe = builder.groupCount(field).build()

        then: "the group count is added for the field"
        pipe[0].$group._id.getAt(field) == "\$$field"
        pipe[0].$group.count == [$sum: 1]

    }

    void "should add group count with non-default sum"() {
        given: "I have a field"
        def field = 'field'
        def sum = 0

        when: "I add a group count with non-default sum"
        def pipe = builder.groupCount(field, sum).build()

        then: "the group count is added for the field"
        pipe[0].$group._id.getAt(field) == "\$$field"
        pipe[0].$group.count == [$sum: sum]

    }

    void "should add group count with sum and document field"() {
        given: "I have a field"
        def field = 'field'
        def docField = 'subdoc.field'
        def sum = 0

        when: "I add a group count with non-default sum"
        def pipe = builder.groupCount(field, sum, docField).build()

        then: "the group count is added for the field"
        pipe[0].$group._id.getAt(field) == "\$$docField"
        pipe[0].$group.count == [$sum: sum]

    }

    void "should copy a given list to a new QueryPipeBuilder"() {
        given: "I have a list as query pipeline"
        def queryPipeline = new QueryPipeBuilder().match('field', 'value').build()

        when: "I create a copy of the pipe"
        def copiedPipeline = QueryPipeBuilder.copy(queryPipeline)

        then: "the new query pipeline is the same as the copied version"
        copiedPipeline.build() == queryPipeline

    }

    void "should copy pipeline list to new query pipeline, but not be the same instance"() {
        given: "I have a list as query pipeline"
        def queryPipeline = new QueryPipeBuilder().match('field', 'value').build()

        when: "I create a copy of the pipe"
        def copiedPipeline = QueryPipeBuilder.copy(queryPipeline)

        and:
        copiedPipeline.match('otherField', 'value')

        then: "the new query pipeline is the same as the copied version"
        copiedPipeline.build() != queryPipeline
    }

    void "should create a count pipeline entry"() {
        when: "I do a count on the pipeline"
        def count = new QueryPipeBuilder().count().build()

        then: "the count is added"
        count[0] == [$count: 'count']
    }

    void "should create a skip pipeline entry"() {
        given: "I have a number of documents to skip"
        def skip = 5

        when: "I do a skip on the pipeline"
        def count = new QueryPipeBuilder().skip(skip).build()

        then: "the skip is added"
        count[0] == [$skip: skip]
    }

    void "should throw exception when skip is negative"() {
        given: "I have a negative number of documents to skip"
        def skip = -5

        when: "I do a skip on the pipeline"
        def count = new QueryPipeBuilder().skip(skip).build()

        then: "the skip is added"
        def e = thrown(IllegalArgumentException)
        e.message == "Parameter 'skip' must be positive integer"
    }

    void "should match field exists true"() {
        given: "I have a field"
        def field = 'field'

        when: "I mactch exists"
        def pipe = builder.matchExists(field).build()

        then: "the match exists true is added to the pipeline"
        pipe[0].$match.getAt(field).$exists
    }

    void "should match field exists false"() {
        given: "I have a field"
        def field = 'field'
        def exists = false

        when: "I mactch exists"
        def pipe = builder.matchExists(field,exists).build()

        then: "the match exists true is added to the pipeline"
        pipe[0].$match.getAt(field).$exists == exists
    }

    void "should match field exists true, explicit"() {
        given: "I have a field"
        def field = 'field'
        def exists = true

        when: "I mactch exists"
        def pipe = builder.matchExists(field,exists).build()

        then: "the match exists true is added to the pipeline"
        pipe[0].$match.getAt(field).$exists == exists
    }

    void "should throw exception if no match created"() {
        when: "I have created a match"
        builder.match().build()

        then: "an exception is thrown"
        def e = thrown(IllegalStateException)
        e.message == 'Match is not defined'
    }

    void "should add filter to pipeline"() {
        given: "I have a field to filter"
        def field = 'field'

        when: "I add a filter"
        def filter = builder.filter(field)

        then: "an instance of filter is returned"
        filter instanceof Filter

    }

    void "should add all DBObjects in a list to the query pipeline"() {
        given: "I have a list of DBObjects"
        def list = [new BasicDBObject('field', 'value'), new BasicDBObject('field2', 'value2'), [field3: 'value3']]

        when: "I addAll to the pipeline"
        def pipe = builder.addAll(list).build()

        then: "the pipe has the values in the list"
        pipe.size() == 2
        pipe[0].field == 'value'
        pipe[1].field2 == 'value2'
    }

    void "should add  DBObjects in a list to the query pipeline with leftShift"() {
        given: "I have a list of DBObjects"
        DBObject dbObject = new BasicDBObject('field', 'value')

        when: "I addAll to the pipeline"
        builder << dbObject
        def pipe = builder.build()

        then: "the pipe has the values in the list"
        pipe.size() == 1
        pipe[0].field == 'value'
    }

    void "should add all DBObjects in a list to the query pipeline with leftShift"() {
        given: "I have a list of DBObjects"
        def list = [new BasicDBObject('field', 'value'), new BasicDBObject('field2', 'value2'), [field3: 'value3']]

        when: "I addAll to the pipeline"
        builder << list
        def pipe = builder.build()

        then: "the pipe has the values in the list"
        pipe.size() == 2
        pipe[0].field == 'value'
        pipe[1].field2 == 'value2'
    }
}

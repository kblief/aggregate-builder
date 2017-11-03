/*******************************************************************************
 * Copyright (c) Dodge Data & Analytics 2016 - 2017
 ******************************************************************************/

package com.curtkurt.mongo

import spock.lang.Specification
import spock.lang.Subject

class MatchSpec extends Specification {

    private QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should create a match equals on the field and value"() {
        given: "I have a field and value"
        def field = 'field'
        def value = 'value'

        when: "I call equal"
        def result = builder.match().equal(field, value).build()

        then: "the result contains a match on the field and value"
        result[0].$match.getAt(field) == value
    }

    void "should create a null match when doesNotExist is called"() {
        given: "I have a field"
        def field = 'field'

        when: "I call isNull"
        def result = builder.match().isNull(field).build()

        then: "the result has a match on field to null"
        result[0].$match.getAt(field) == null
    }

    void "should create a match on field to list of values"() {
        given: "I have a field and a list of values"
        def field = 'field'
        def listOfValues = ['a', 'b', 'c']

        when: "I build a querypipe to match field to list of values"
        def result = builder.match().inList(field, listOfValues).build()

        then: "the querypipe contains a match on field to list of values"
        result[0].$match.getAt(field).$in == listOfValues
    }

    void "should match between numeric values"() {
        given: "I have an upper and lower bound number and a field"
        int lower = 1
        int upper  = 99
        def field = 'field'

        when: "I match between values"
        def pipe = builder.match().betweenRange(field, new IntegerRange(upper,lower)).build()

        then: "the pipe should contain a match and ge and le on lower and upper bound"
        pipe[0].$match.$and[0].getAt(field).$gt == lower
        pipe[0].$match.$and[1].getAt(field).$lt == upper

    }

    void "should match between dates lower bound inclusive"() {
        given: "I have a field, lower range date, and upper date"
        def field = 'field'
        def lower = Date.parse('yyyy-MM-dd','2017-07-20')
        def upper = lower + 1

        when: "I match between values lower inclusive"
        def pipe = builder.match().betweenRangeLowerInclusive(field,new DateRange(upper,lower)).build()

        then: "I get a match between lower and upper, lower inclusive"
        pipe[0].$match.$and[0].getAt(field).$gte == lower
        pipe[0].$match.$and[1].getAt(field).$lt == upper
    }

    void "should match between dates upper bound inclusive"() {
        given: "I have a field, lower range date, and upper date"
        def field = 'field'
        def lower = Date.parse('yyyy-MM-dd','2017-07-20')
        def upper = lower + 1

        when: "I match between values lower inclusive"
        def pipe = builder.match().betweenRangeUpperInclusive(field,new DateRange(upper,lower)).build()

        then: "I get a match between lower and upper, lower inclusive"
        pipe[0].$match.$and[0].getAt(field).$gt == lower
        pipe[0].$match.$and[1].getAt(field).$lte == upper
    }

    void "should match between dates inclusive"() {
        given: "I have a field, lower range date, and upper date"
        def field = 'field'
        def lower = Date.parse('yyyy-MM-dd','2017-07-20')
        def upper = lower + 1

        when: "I match between values lower inclusive"
        def pipe = builder.match().betweenRangeInclusive(field,new DateRange(upper,lower)).build()

        then: "I get a match between lower and upper, lower inclusive"
        pipe[0].$match.$and[0].getAt(field).$gte == lower
        pipe[0].$match.$and[1].getAt(field).$lte == upper
    }

    void "should get an instance of orList when or is called"() {
        when: "I call or"
        def or = builder.match().or()

        then: "or is instance of Orlist"
        or instanceof OrList
    }

    void "should get an instance of andList when and is called"() {
        when: "I call and"
        def and = builder.match().and()

        then: "or is instance of Andlist"
        and instanceof AndList
    }

    void "should match field exists true"() {
        given: "I have a field"
        def field = 'field'

        when: "I mactch exists"
        def pipe = builder.match().exists(field).build()

        then: "the match exists true is added to the pipeline"
        pipe[0].$match.getAt(field).$exists
    }

    void "should match field exists false"() {
        given: "I have a field"
        def field = 'field'
        def exists = false

        when: "I mactch exists"
        def pipe = builder.match().exists(field,exists).build()

        then: "the match exists true is added to the pipeline"
        pipe[0].$match.getAt(field).$exists == exists
    }

    void "should match field exists true, explicit"() {
        given: "I have a field"
        def field = 'field'
        def exists = true

        when: "I mactch exists"
        def pipe = builder.match().exists(field,exists).build()

        then: "the match exists true is added to the pipeline"
        pipe[0].$match.getAt(field).$exists == exists
    }

    void "should do an elemMatch on a field"() {
        given: "I have an element to match to a field"
        def field = 'field'
        def element = [a:'a_value',b:'b_value']

        when: "I create an elemMatch"
        def pipe = builder.match().elemMatch(field,element).build()

        then: "the pipe contains an elemMatch on the field"
        pipe[0].$match.getAt(field).$elemMatch == element
    }

    void "should add a match ne value to pipeline"() {
        given: "I have an field to not match"
        def field = 'field'
        def value = 'value'

        when: "I create an elemMatch"
        def pipe = builder.match().notEqual(field,value).build()

        then: "the pipe contains an elemMatch on the field"
        pipe[0].$match.getAt(field).$ne == value
    }
}

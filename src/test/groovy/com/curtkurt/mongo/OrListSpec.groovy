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

import spock.lang.Specification

class OrListSpec extends Specification {

    def builder = new QueryPipeBuilder()

    void "should create an or entry of greater than"() {
        given: "I have a field"
        def field = 'field'
        def value = 100

        when: "I do an and().gt"
        def result = builder.match().or().gt(field, value).end().build()

        then: "I have an or entry of field gt value"
        result[0].$match.$or[0].getAt(field).$gt == value
    }

    void "should create an or equals"() {
        given: "I have a field and value"
        def field = 'field'
        def value = 'value'

        when: "I call equal"
        def result = builder.match().or().eq(field, value).end().build()

        then: "I get an or with field to value"
        result[0].$match.$or[0].getAt(field) == value
    }

    void "should create a in list or"() {
        given: "I have a field and a list of values"
        def field = 'field'
        def values = ['a','b','c']

        when: "I do an or inList"
        def result = builder.match().or().inList(field, values).end().build()

        then: "I have entries in my or list for the field and values"
        result[0].$match.$or[0].getAt(field) == values[0]
        result[0].$match.$or[1].getAt(field) == values[1]
        result[0].$match.$or[2].getAt(field) == values[2]

    }

    void "should create an AndList object when and is called"() {
        when: "I call and"
        def and = builder.match().or().and()

        then: "I get an AndList object"
        and instanceof AndList
    }

    void "should add option to and list"() {
        given: "I have a field and a value"
        def field = 'field'
        def value = 'value'

        when: "I add an option to the and list"
        def result = builder.match().or().addOption(field, value).end().build()

        then: "I have an entry in my and list of field to value"
        result[0].$match.$or[0].getAt(field) == value
    }

    void "should add options to and list"() {
        given: "I have a field and a value"
        def options = [fieldA:'a',fieldB:'b',fieldC:'c']

        when: "I add options to the and list"
        def result = builder.match().or().addOptions(options).end().build()

        then: "I have an entry in my and list of field to value"
        result[0].$match.$or[0].fieldA == options.values()[0]
        result[0].$match.$or[1].fieldB == options.values()[1]
        result[0].$match.$or[2].fieldC == options.values()[2]
    }

    void "should return QueryPipeBuilder when end is called"() {
        when: "I call end on an orList"
        def result = builder.match().or().end()

        then: "the result is the query pipe builder"
        result == builder
    }
}

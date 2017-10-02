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

import spock.lang.Specification


class AndListSpec extends Specification {

    def builder = new QueryPipeBuilder()

    void "should create an and entry of greater than"() {
        given: "I have a field"
        def field = 'field'
        def value = 100

        when: "I do an and().gt"
        def result = builder.match().and().gt(field, value).end().build()

        then: "I have an and entry of field gt value"
        result[0].$match.$and[0].getAt(field).$gt == value
    }

    void "should create an entry for field greater than equal to value"() {
        given: "I have a field and value"
        def field = 'field'
        def value = 100

        when: "I go an and().gte"
        def result = builder.match().and().gte(field, value).end().build()

        then: "I have and entry of a field gte value"
        result[0].$match.$and[0].getAt(field).$gte == value
    }

    void "should create an and entry of less than"() {
        given: "I have a field"
        def field = 'field'
        def value = 100

        when: "I do an and().lt"
        def result = builder.match().and().lt(field, value).end().build()

        then: "I have an and entry of field lt value"
        result[0].$match.$and[0].getAt(field).$lt == value
    }

    void "should create an entry for field less than equal to value"() {
        given: "I have a field and value"
        def field = 'field'
        def value = 100

        when: "I go an and().lte"
        def result = builder.match().and().lte(field, value).end().build()

        then: "I have and entry of a field lte value"
        result[0].$match.$and[0].getAt(field).$lte == value
    }

    void "should add option to and list"() {
        given: "I have a field and a value"
        def field = 'field'
        def value = 'value'

        when: "I add an option to the and list"
        def result = builder.match().and().addOption(field, value).end().build()

        then: "I have an entry in my and list of field to value"
        result[0].$match.$and[0].getAt(field) == value
    }

    void "should add options to and list"() {
        given: "I have a field and a value"
        def options = [fieldA:'a',fieldB:'b',fieldC:'c']

        when: "I add options to the and list"
        def result = builder.match().and().addOptions(options).end().build()

        then: "I have an entry in my and list of field to value"
        result[0].$match.$and[0].fieldA == options.values()[0]
        result[0].$match.$and[1].fieldB == options.values()[1]
        result[0].$match.$and[2].fieldC == options.values()[2]
    }

    void "should return QueryPipeBuilder when end is called"() {
        when: "I call end on an andList"
        def result = builder.match().and().end()

        then: "the result is the query pipe builder"
        result == builder
    }
}

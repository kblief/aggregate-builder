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

class FilterSpec extends Specification {

    private QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should create filter on pipeline"() {
        given: "I have a field"
        def field = 'field'
        def arrayField = '_field'
        def value = 'value'

        when: "I create a filter on the pipeline"
        def pipeline = builder.filter(field,arrayField).cond().eq(arrayField, value).build()

        then: "the pipeline has a filter on field, arrayField for value"
        pipeline[0].$filter.input == '$' + field
        pipeline[0].$filter.as == '_' + field
        def cond = pipeline[0].$filter.cond
        cond.$eq == ['$$_field', value]
    }

    void "should add condition to filter"() {
        given: "I have an array field"
        def field = 'field'

        when: "I add filter to projection with condition"
        def condition = builder.projection().filter(field).cond()

        then: "the condition is returned"
        condition instanceof Condition
    }

    void "should add multiple filters to projection"() {
        given: "I have a fieldName"
        def filterField = 'filterField'
        def filterAsField = '_' + filterField
        def value = 'value'
        def field = 'field'

        and: "I have another field name"
        def anotherFilterField = 'anotherFilterField'
        def anotherField = 'anotherField'
        def anotherFilterAsField = '_' + anotherFilterField
        def anotherValue = 'anotherValue'

        when: "I create a projection wtth two filters"
        def pipe = builder.projection()
                .filter(filterField,filterAsField)
                .cond()
                .and().eq(field, value).close()
                .filter(anotherFilterField,anotherFilterAsField)
                .cond()
                .and().eq(anotherField, anotherValue).end()
                .build()

        then: "there is a filter for filter field"
        pipe[0].$project.getAt(filterField).$filter.input == '$' + filterField
        pipe[0].$project.getAt(filterField).$filter.as == '_' + filterField
        pipe[0].$project.getAt(filterField).$filter.cond.$and[0].$eq == ['$$' + field, value]

        and: "there is a filter for anotherFilterField"
        pipe[0].$project.getAt(anotherFilterField).$filter.input == '$' + anotherFilterField
        pipe[0].$project.getAt(anotherFilterField).$filter.as == '_' + anotherFilterField
        pipe[0].$project.getAt(anotherFilterField).$filter.cond.$and[0].$eq == ['$$' + anotherField, anotherValue]
    }

    void "should add multiple size with filters and an andList with orList inside to projection"() {
        given: "I have a fieldName"
        def filterField = 'filterField'
        def filterAsField = '_' + filterField
        def value = 'value'
        def field = 'field'
        def bField = 'bField'
        def bValue = 'bValue'
        def sizeField = 'sizeField'

        and: "I have another field name"
        def anotherFilterField = 'anotherFilterField'
        def anotherFilterAsField = '_' + anotherFilterField
        def anotherField = 'anotherField'
        def anotherValue = 'anotherValue'
        def anotherBField = 'anotherBField'
        def anotherBValue = 'anotherBValue'
        def anotherSizeField = 'anotherSizeField'

        when: "I create a projection with two filters"
        def pipe = builder.projection()
                .size(sizeField)
                .filter(filterField,filterAsField)
                .cond()
                .and()
                .or()
                .eq(field, value)
                .eq(bField,bValue)
                .close()
                .size(anotherSizeField)
                .filter(anotherFilterField,anotherFilterAsField)
                .cond()
                .and()
                .or()
                .eq(anotherField, anotherValue)
                .eq(anotherBField, anotherBValue)
                .end()
                .build()

        then: "there is a filter for filter field"
        pipe[0].$project.getAt(sizeField).$size.$filter.input == '$' + filterField
        pipe[0].$project.getAt(sizeField).$size.$filter.as == '_' + filterField
        pipe[0].$project.getAt(sizeField).$size.$filter.cond.$and[0].$or[0].$eq == ['$$' + field, value]
        pipe[0].$project.getAt(sizeField).$size.$filter.cond.$and[0].$or[1].$eq == ['$$' + bField, bValue]

        and: "there is a filter for anotherFilterField"
        pipe[0].$project.getAt(anotherSizeField).$size.$filter.input == '$' + anotherFilterField
        pipe[0].$project.getAt(anotherSizeField).$size.$filter.as == '_' + anotherFilterField
        pipe[0].$project.getAt(anotherSizeField).$size.$filter.cond.$and[0].$or[0].$eq == ['$$' + anotherField, anotherValue]
        pipe[0].$project.getAt(anotherSizeField).$size.$filter.cond.$and[0].$or[1].$eq == ['$$' + anotherBField, anotherBValue]
    }

    void "should throw exception if build is called before filter is added"() {
        when: "I create a filter and build without a filter conditions"
        builder.filter('field').build()

        then: "an exception is thrown"
        def e = thrown(IllegalStateException)
        e.message == 'Filter requires an array to operate on with a condition'
    }
}

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


class ConditionSpec extends Specification {

    QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should create condition on filter in pipeline for eq"() {
        given: "I have a field"
        def field = 'field'
        def arrayField = '_field'
        def value = 'value'

        when: "I create a filter on the pipeline"
        def pipeline = builder.filter(field,arrayField).cond().eq(arrayField, value).build()

        then: "the pipeline has a filter on field, arrayField for value"
        pipeline[0].$filter.input == '$' + field
        pipeline[0].$filter.as == '_' + field
        pipeline[0].$filter.cond.$eq == ['$$_field', value]
    }

    void "should create condition on filter in pipeline for gt"() {
        given: "I have a field"
        def field = 'field'
        def arrayField = '_field'
        def value = 'value'

        when: "I create a filter on the pipeline"
        def pipeline = builder.filter(field,arrayField).cond().gt(arrayField, value).build()

        then: "the pipeline has a filter on field, arrayField for value"
        pipeline[0].$filter.input == '$' + field
        pipeline[0].$filter.as == '_' + field
        pipeline[0].$filter.cond.$gt == ['$$_field', value]
    }

    void "should create condition on filter in pipeline for lt"() {
        given: "I have a field"
        def field = 'field'
        def arrayField = '_field'
        def value = 'value'

        when: "I create a filter on the pipeline"
        def pipeline = builder.filter(field,arrayField).cond().lt(arrayField, value).build()

        then: "the pipeline has a filter on field, arrayField for value"
        pipeline[0].$filter.input == '$' + field
        pipeline[0].$filter.as == '_' + field
        pipeline[0].$filter.cond.$lt == ['$$_field', value]
    }

    void "should create condition on filter in pipeline for gte"() {
        given: "I have a field"
        def field = 'field'
        def arrayField = '_field'
        def value = 'value'

        when: "I create a filter on the pipeline"
        def pipeline = builder.filter(field, arrayField).cond().gte(arrayField, value).build()

        then: "the pipeline has a filter on field, arrayField for value"
        pipeline[0].$filter.input == '$' + field
        pipeline[0].$filter.as == '_' + field
        pipeline[0].$filter.cond.$gte == ['$$_field', value]
    }

    void "should create condition on filter in pipeline for lte"() {
        given: "I have a field"
        def field = 'field'
        def arrayField = '_field'
        def value = 'value'

        when: "I create a filter on the pipeline"
        def pipeline = builder.filter(field,arrayField).cond().lte(arrayField, value).build()

        then: "the pipeline has a filter on field, arrayField for value"
        pipeline[0].$filter.input == '$' + field
        pipeline[0].$filter.as == '_' + field
        pipeline[0].$filter.cond.$lte == ['$$_field', value]
    }
}

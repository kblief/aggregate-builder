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


class CondOrListSpec extends Specification {

    QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should add a condition to projection filter with eq"() {
        given: "I have a field and value"
        def condField = '_field.field'
        def field = 'field'
        def value = 4

        when: "I add a condition to a projection"
        def pipeline = builder.projection().filter(field).cond().or().eq(condField, value).end().build()

        then: "the pipeline has a projection with filter and condition"
        pipeline[0].$project.getAt(field).$filter.cond.$or[0].$eq == ['$$' + condField, value]
    }

    void "should add a condition to projection filter with lt"() {
        given: "I have a field and value"
        def condField = '_field.field'
        def field = 'field'
        def value = 4

        when: "I add a condition to a projection"
        def pipeline = builder.projection().filter(field).cond().or().lt(condField, value).end().build()

        then: "the pipeline has a projection with filter and condition"
        pipeline[0].$project.getAt(field).$filter.cond.$or[0].$lt == ['$$' + condField, value]
    }

    void "should add a condition to projection filter with gt"() {
        given: "I have a field and value"
        def condField = '_field.field'
        def field = 'field'
        def value = 4

        when: "I add a condition to a projection"
        def pipeline = builder.projection().filter(field).cond().or().gt(condField, value).end().build()

        then: "the pipeline has a projection with filter and condition"
        pipeline[0].$project.getAt(field).$filter.cond.$or[0].$gt == ['$$' + condField, value]
    }

    void "should add a condition to projection filter with lte"() {
        given: "I have a field and value"
        def condField = '_field.field'
        def field = 'field'
        def value = 4

        when: "I add a condition to a projection"
        def pipeline = builder.projection().filter(field).cond().or().lte(condField, value).end().build()

        then: "the pipeline has a projection with filter and condition"
        pipeline[0].$project.getAt(field).$filter.cond.$or[0].$lte == ['$$' + condField, value]
    }

    void "should add a condition to projection filter with gte"() {
        given: "I have a field and value"
        def condField = '_field.field'
        def field = 'field'
        def value = 4

        when: "I add a condition to a projection"
        def pipeline = builder.projection().filter(field).cond().or().gte(condField, value).end().build()

        then: "the pipeline has a projection with filter and condition"
        pipeline[0].$project.getAt(field).$filter.cond.$or[0].$gte == ['$$' + condField, value]
    }
}

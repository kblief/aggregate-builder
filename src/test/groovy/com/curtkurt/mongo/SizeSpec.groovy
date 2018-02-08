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


class SizeSpec extends Specification {

    private QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should return a filter instance when filter is called"() {
        given: "I have a field name for size"
        def field = 'field'

        and: "I have an array field I want to filter on"
        def arrayField = 'array.field'

        when: "I create a filter in size"
        def filter = builder.projection().size(field).filter(arrayField)

        then: "an instance of filter is returned"
        filter instanceof Filter
    }

    void "should build a size on projection when size is called with fieldname and field"() {
        given: "I have a field and fieldname"
        def field = 'field'
        def fieldName = 'fieldName'

        when: "I add a size to a projection with field name and field"
        def pipe = builder.projection().size(fieldName, field).end().build()

        then: "the size is added to the pipeline"
        pipe[0].$project.getAt(fieldName).$size == '$' + field
    }

    void "should create a size on the projection of the pipeline"() {
        given: "I have a fieldName for size"
        def fieldName = 'sizeField'
        def aField = 'aField'

        when: "I create a size on the projection"
        def pipe = builder.projection().size(fieldName).size(aField).end().build()

        then: "the pipe has a size entry in the projection"
        pipe[0].$project.getAt(fieldName).$size == '$' + aField
    }

    void "should throw an exception id size is not defined"() {
        given: "I have a field name"
        def fieldName = 'fieldName'

        when: "I create a size with no conditions"
        builder.projection().size(fieldName).build()

        then: "an exception is thrown"
        def e = thrown(IllegalStateException)
        e.message == 'Size is not defined'
    }
}

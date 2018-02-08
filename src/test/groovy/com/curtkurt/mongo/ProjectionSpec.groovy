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

class ProjectionSpec extends Specification {

    private QueryPipeBuilder builder = new QueryPipeBuilder()

    void "should add projection of fields"() {
        given: "I have a map of fields"
        def fields = [field: 1, otherField: 1, hiddenField: 0]

        when: "I add a projection"
        def pipe = builder.projection().project(fields).end().build()

        then: "the result has a \$project added with the fields"
        pipe[0].$project.field == 1
        pipe[0].$project.otherField == 1
        pipe[0].$project.hiddenField == 0
    }

    void "should throw exception if build is called before project is added"() {
        when: "I create a projection and build without a project"
        builder.projection().build()

        then: "an exception is thrown"
        def e = thrown(IllegalStateException)
        e.message == 'Projection is not defined'
    }

    void "should create a size class when size is called"() {
        given: "I have a field that I want assigned to size"
        def sizeField = 'sizeField'

        when: "I create a projection with size"
        def size = builder.projection().size(sizeField)

        then: "a size instance is returned"
        size instanceof Size
    }
}

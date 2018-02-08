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

import com.mongodb.client.model.geojson.Position
import spock.lang.Specification

class GeoNearSpec extends Specification {

    QueryPipeBuilder builder = new QueryPipeBuilder()


    void "should create a geoNear aggregation pipeline"() {
        given: "I have a field with point"
        def field = 'location'

        when: "I create a geoNear"
        def position = new Position(40.001, -70.001)
        def pipe = builder.geoNear().distanceFromPosition(field, 1.0d, position, GeoNear.UnitsOfLength.KILOMETERS).build()

        then: "the pipeline has a geoNear query"
        pipe[0].$geoNear.near.type == 'Point'
        pipe[0].$geoNear.near.coordinates == position.values
        pipe[0].$geoNear.maxDistance == 1000
        pipe[0].$geoNear.distanceField == 'distance'
        pipe[0].$geoNear.includeLocs == field
        pipe[0].$geoNear.spherical == true

    }
}

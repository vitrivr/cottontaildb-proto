package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseSchema
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DropSchema(name: String) {
    /** Internal [CottontailGrpc.DropSchemaMessage.Builder]. */
    val builder = CottontailGrpc.DropSchemaMessage.newBuilder()

    init {
        this.builder.schema = name.parseSchema()
    }
}
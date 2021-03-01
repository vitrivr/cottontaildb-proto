package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class CreateSchema(name: String) {
    /** Internal [CottontailGrpc.CreateSchemaMessage.Builder]. */
    val builder = CottontailGrpc.CreateSchemaMessage.newBuilder()

    init {
        this.builder.schemaBuilder.name = name
    }
}
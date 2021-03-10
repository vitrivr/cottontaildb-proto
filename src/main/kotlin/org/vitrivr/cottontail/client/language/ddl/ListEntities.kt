package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all entities in a schema.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListEntities(val name: String) {
    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    val builder = CottontailGrpc.ListEntityMessage.newBuilder()
    init {
        this.builder.schemaBuilder.setName(name)
    }
}
package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseSchema
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all entities in a schema.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListEntities(name: String) {

    /** Internal [CottontailGrpc.ListEntityMessage.Builder]. */
    val builder = CottontailGrpc.ListEntityMessage.newBuilder()
    init {
        this.builder.schema = name.parseSchema()
    }

    /**
     * Sets the transaction ID for this [ListEntities].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): ListEntities {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [ListEntities].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): ListEntities {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }
}
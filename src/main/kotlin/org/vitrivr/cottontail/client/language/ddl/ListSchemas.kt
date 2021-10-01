package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to list all schemas.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class ListSchemas {
    /** Internal [CottontailGrpc.ListSchemaMessage.Builder]. */
    val builder = CottontailGrpc.ListSchemaMessage.newBuilder()

    /**
     * Sets the transaction ID for this [ListSchemas].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): ListSchemas {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [ListSchemas].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): ListSchemas {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }
}
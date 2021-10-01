package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseSchema
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class CreateSchema(name: String) {
    /** Internal [CottontailGrpc.CreateSchemaMessage.Builder]. */
    val builder = CottontailGrpc.CreateSchemaMessage.newBuilder()

    init {
        this.builder.schema = name.parseSchema()
    }

    /**
     * Sets the transaction ID for this [CreateSchema].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): CreateSchema {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [CreateSchema].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): CreateSchema {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }
}
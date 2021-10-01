package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseIndex
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DropIndex(name: String) {
    val builder = CottontailGrpc.DropIndexMessage.newBuilder()

    init {
        this.builder.index = name.parseIndex()
    }

    /**
     * Sets the transaction ID for this [DropIndex].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): DropIndex {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [DropIndex].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): DropIndex {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }
}
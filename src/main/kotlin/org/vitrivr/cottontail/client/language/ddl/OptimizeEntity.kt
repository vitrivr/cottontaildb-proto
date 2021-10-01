package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.dml.BatchInsert
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A OPTIMIZE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class OptimizeEntity(name: String) {
    /** Internal [CottontailGrpc.OptimizeEntityMessage.Builder]. */
    val builder = CottontailGrpc.OptimizeEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [OptimizeEntity].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): OptimizeEntity {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [OptimizeEntity].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): OptimizeEntity {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }
}
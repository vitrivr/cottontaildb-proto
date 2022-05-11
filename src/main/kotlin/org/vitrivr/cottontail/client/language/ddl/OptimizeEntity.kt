package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An OPTIMIZE ENTITY query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class OptimizeEntity(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.OptimizeEntityMessage.Builder]. */
    internal val builder = CottontailGrpc.OptimizeEntityMessage.newBuilder()

    init {
        builder.entity = name.parseEntity()
    }

    /**
     * Sets the transaction ID for this [OptimizeEntity].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): OptimizeEntity {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [OptimizeEntity].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): OptimizeEntity {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [OptimizeEntity]
     *
     * @return The size in bytes of this [OptimizeEntity].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}
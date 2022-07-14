package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A message to query information about a column.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AboutColumn(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.ColumnDetailsMessage.Builder]. */
    internal val builder = CottontailGrpc.ColumnDetailsMessage.newBuilder()

    init {
        this.builder.column = name.parseColumn()
    }

    /**
     * Sets the transaction ID for this [AboutColumn].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): AboutColumn {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [AboutColumn].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): AboutColumn {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Returns the serialized message size in bytes of this [AboutColumn]
     *
     * @return The size in bytes of this [AboutColumn].
     */
    override fun serializedSize() = this.builder.build().serializedSize
}
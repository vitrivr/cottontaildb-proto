package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseSchema
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP SCHEMA query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class DropSchema(name: String): LanguageFeature() {
    /** Internal [CottontailGrpc.DropSchemaMessage.Builder]. */
    val builder = CottontailGrpc.DropSchemaMessage.newBuilder()

    init {
        this.builder.schema = name.parseSchema()
    }

    /**
     * Sets the transaction ID for this [DropSchema].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): DropSchema {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [DropSchema].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): DropSchema {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }
}
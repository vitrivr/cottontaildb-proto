package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A CREATE INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class CreateIndex(entity: String, column: String, type: CottontailGrpc.IndexType): LanguageFeature() {

    val builder = CottontailGrpc.CreateIndexMessage.newBuilder()
    init {
        this.builder.entity = entity.parseEntity()
        this.builder.addColumns(column.parseColumn())
        this.builder.type = type
    }

    /**
     * Sets the transaction ID for this [CreateIndex].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): CreateIndex {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [CreateIndex].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): CreateIndex {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a column to this [CreateIndex].
     *
     * @param column The name of the column
     * @return this [CreateIndex]
     */
    fun column(column: String): CreateIndex {
        this.builder.addColumns(column.parseColumn())
        return this
    }

    /**
     * Adds a index creation parameter to this [CreateIndex].
     *
     * @param key The name of the parameter
     * @param value The value of the parameter
     * @return this [CreateIndex]
     */
    fun param(key: String, value: Any): CreateIndex {
        this.builder.putParams(key, value.toString())
        return this
    }


    /**
     * Sets the rebuild flag of this [CreateIndex].
     *
     * @return this [CreateIndex]
     */
    fun rebuild(): CreateIndex {
        this.builder.rebuild = true
        return this
    }
}
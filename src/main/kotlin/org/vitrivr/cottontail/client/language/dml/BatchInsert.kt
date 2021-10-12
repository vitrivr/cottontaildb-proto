package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.basics.LanguageFeature
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A BATCH INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class BatchInsert(entity: String? = null): LanguageFeature() {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    val builder = CottontailGrpc.BatchInsertMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [BatchInsert].
     *
     * @param txId The new transaction ID.
     */
    override fun txId(txId: Long): BatchInsert {
        this.builder.metadataBuilder.transactionId = txId
        return this
    }

    /**
     * Sets the query ID for this [BatchInsert].
     *
     * @param queryId The new query ID.
     */
    override fun queryId(queryId: String): BatchInsert {
        this.builder.metadataBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a FROM-clause to this [BatchInsert].
     *
     * @param entity The name of the entity to [BatchInsert] to.
     * @return This [BatchInsert]
     */
    fun into(entity: String): BatchInsert {
        this.builder.clearFrom()
        this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a column to this [BatchInsert].
     *
     * @param columns The name of the columns this [BatchInsert] should insert into.
     * @return This [BatchInsert]
     */
    fun columns(vararg columns: String): BatchInsert {
        this.builder.clearColumns()
        for (c in columns) {
            this.builder.addColumns(c.parseColumn())
        }
        return this
    }

    /**
     * Appends values to this [BatchInsert].
     *
     * @param values The value to append to the [BatchInsert]
     * @return This [BatchInsert]
     */
    fun append(vararg values: Any?): BatchInsert {
        val insert = CottontailGrpc.BatchInsertMessage.Insert.newBuilder()
        for (v in values) {
            insert.addValues(v?.convert() ?: CottontailGrpc.Literal.newBuilder().build())
        }
        this.builder.addInserts(insert)
        return this
    }

    /**
     * Calculates and returns the size of this [BatchInsert]
     *
     * @return The size in bytes of this [BatchInsert].
     */
    fun size() = this.builder.build().serializedSize
}
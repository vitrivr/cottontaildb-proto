package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.extensions.*
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DELETE query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class Delete(entity: String? = null) {
    /** Internal [CottontailGrpc.DeleteMessage.Builder]. */
    val builder = CottontailGrpc.DeleteMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [Delete].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): Delete {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [Delete].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): Delete {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a FROM-clause to this [Delete].
     *
     * @param entity The name of the entity to [Delete] from.
     * @return This [Delete]
     */
    fun from(entity: String): Delete {
        this.builder.clearFrom()
        this.builder.setFrom(
            CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a WHERE-clause to this [Delete].
     *
     * @return This [Delete]
     */
    fun where(predicate: Predicate): Delete {
        this.builder.clearWhere()
        val builder = this.builder.whereBuilder
        when(predicate) {
            is Atomic -> builder.setAtomic(predicate.toPredicate())
            is And -> builder.setCompound(predicate.toPredicate())
            is Or -> builder.setCompound(predicate.toPredicate())
        }
        return this
    }
}
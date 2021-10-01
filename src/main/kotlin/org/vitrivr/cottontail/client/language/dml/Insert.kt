package org.vitrivr.cottontail.client.language.dml

import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseEntity
import org.vitrivr.cottontail.client.language.extensions.toLiteral
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A INSERT query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.2.0
 */
class Insert(entity: String? = null) {
    /** Internal [CottontailGrpc.InsertMessage.Builder]. */
    val builder = CottontailGrpc.InsertMessage.newBuilder()

    init {
        if (entity != null) {
            this.builder.setFrom(CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        }
    }

    /**
     * Sets the transaction ID for this [Update].
     *
     * @param txId The new transaction ID.
     */
    fun txId(txId: Long): Insert {
        this.builder.txIdBuilder.value = txId
        return this
    }

    /**
     * Sets the query ID for this [Update].
     *
     * @param queryId The new query ID.
     */
    fun queryId(queryId: String): Insert {
        this.builder.txIdBuilder.queryId = queryId
        return this
    }

    /**
     * Adds a FROM-clause to this [Insert].
     *
     * @param entity The name of the entity to [Insert] to.
     * @return This [Insert]
     */
    fun into(entity: String): Insert {
        this.builder.clearFrom()
        this.builder.setFrom(
            CottontailGrpc.From.newBuilder().setScan(CottontailGrpc.Scan.newBuilder().setEntity(entity.parseEntity())))
        return this
    }

    /**
     * Adds a value assignments this [Insert]. This method is cumulative, i.e., invoking
     * this method multiple times appends another assignment each time.
     *
     * @param column The name of the column to insert into.
     * @param value The value or null.
     * @return This [Insert]
     */
    fun value(column: String, value: Any?): Insert {
        this.builder.addElements(
            CottontailGrpc.InsertMessage.InsertElement.newBuilder()
                .setColumn(column.parseColumn())
                .setValue(value?.convert() ?: CottontailGrpc.Literal.newBuilder().build()))
        return this
    }

    /**
     * Adds value assignments this [Insert]. A value assignment consists of a column name and a value.
     *
     * @param assignments The value assignments for the [Insert]
     * @return This [Insert]
     */
    fun values(vararg assignments: Pair<String,Any?>): Insert {
        this.builder.clearElements()
        for (assignment in assignments) {
            this.value(assignment.first, assignment.second)
        }
        return this
    }

    /**
     * Converts an [Any] to a [CottontailGrpc.Literal]
     *
     * @return [CottontailGrpc.Literal]
     */
    private fun Any.convert(): CottontailGrpc.Literal = when(this) {
        is Array<*> -> (this as Array<Number>).toLiteral()
        is BooleanArray -> this.toLiteral()
        is IntArray -> this.toLiteral()
        is LongArray -> this.toLiteral()
        is FloatArray -> this.toLiteral()
        is DoubleArray -> this.toLiteral()
        is Boolean -> this.toLiteral()
        is Byte -> this.toLiteral()
        is Short -> this.toLiteral()
        is Int -> this.toLiteral()
        is Long -> this.toLiteral()
        is Float -> this.toLiteral()
        is Double -> this.toLiteral()
        is String -> this.toLiteral()
        else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to literal is not supported.")
    }
}
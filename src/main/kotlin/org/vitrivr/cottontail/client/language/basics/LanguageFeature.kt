package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.client.language.extensions.toLiteral
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [LanguageFeature] provided by the Cottontail DB simple API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class LanguageFeature {
    /**
     * Sets the transaction ID for this [LanguageFeature].
     *
     * @param txId The new transaction ID.
     */
    abstract fun txId(txId: Long): LanguageFeature

    /**
     * Sets the query ID for this [LanguageFeature].
     *
     * @param queryId The new query ID.
     */
    abstract fun queryId(queryId: String): LanguageFeature

    /**
     * Converts an [Any] to a [CottontailGrpc.Literal]
     *
     * @return [CottontailGrpc.Literal]
     */
    @Suppress("UNCHECKED_CAST")
    protected fun Any.convert(): CottontailGrpc.Literal = when(this) {
        is Array<*> -> {
            require(this[0] is Number) { "Only arrays of numbers can be converted to vector literals." }
            (this as Array<Number>).toLiteral()
        }
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
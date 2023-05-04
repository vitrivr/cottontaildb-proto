package org.vitrivr.cottontail.core.values

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * This is an abstraction over the existing primitive types provided by Kotlin. It allows for the
 * advanced type system implemented by Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
interface Value : Comparable<Value> {
    /**
     * Converts this [Value] to a [CottontailGrpc.Literal] gRCP representation.
     *
     * @return [CottontailGrpc.Literal]
     */
    fun toGrpc(): CottontailGrpc.Literal

    /**
     * Compares two [Value]s. Returns true, if they are equal, and false otherwise.
     *
     * TODO: This method is required because it is currently not possible to override
     * equals() in Kotlin inline classes. Once this changes, this method should be removed.
     *
     * @param other Value to compare to.
     * @return true if equal, false otherwise.
     */
    fun isEqual(other: Value): Boolean
}
package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An [Expression], which can either be a reference to a [Column], a [Literal] value or a [Function] invocation.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
sealed interface Expression {
    /**
     * Converts this [Expression] into a [CottontailGrpc.Expression]
     */
    fun toGrpc(): CottontailGrpc.Expression
}
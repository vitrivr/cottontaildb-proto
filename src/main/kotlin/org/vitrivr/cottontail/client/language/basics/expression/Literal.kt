package org.vitrivr.cottontail.client.language.basics.expression

import org.vitrivr.cottontail.client.language.extensions.toGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Literal] value [Expression].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Literal(val value: Any): Expression {
    override fun toGrpc(): CottontailGrpc.Expression {
        val expression = CottontailGrpc.Expression.newBuilder()
        expression.literal = this.value.toGrpc()
        return expression.build()
    }
}
package org.vitrivr.cottontail.client.language.basics.expression

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.core.types.Value
import org.vitrivr.cottontail.core.values.PublicValue
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Literal] value [Expression].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
@Serializable
@SerialName("Literal")
data class Literal(val value: PublicValue): Expression() {
    override fun toGrpc(): CottontailGrpc.Expression {
        val expression = CottontailGrpc.Expression.newBuilder()
        expression.literal = this.value.toGrpc()
        return expression.build()
    }
}
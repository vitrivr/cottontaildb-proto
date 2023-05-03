package org.vitrivr.cottontail.client.language.basics.expression

import org.vitrivr.cottontail.client.language.extensions.toGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A list of [Literal] values [Expression]. Mainly used for IN queries.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class LiteralList(val value: Array<Any>): Expression {
    override fun toGrpc(): CottontailGrpc.Expression {
        val builder = CottontailGrpc.Expression.newBuilder()
        for (data in this.value) {
            builder.literalListBuilder.addLiteral(this.value.toGrpc())
        }
        return builder.build()
    }
}
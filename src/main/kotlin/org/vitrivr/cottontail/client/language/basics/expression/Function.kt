package org.vitrivr.cottontail.client.language.basics.expression

import org.vitrivr.cottontail.client.language.extensions.parseFunction
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Function] [Expression] used to define a query.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Function(val name: String, vararg val args: Expression): Expression {
    override fun toGrpc(): CottontailGrpc.Expression {
        val function = CottontailGrpc.Function.newBuilder().setName(name.parseFunction())
        for (exp in this.args) {
            function.addArguments(exp.toGrpc())
        }
        return CottontailGrpc.Expression.newBuilder().setFunction(function).build()
    }
}
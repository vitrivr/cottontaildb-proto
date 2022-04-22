package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseFunction
import org.vitrivr.cottontail.client.language.extensions.toGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An [Expression], which can either be a reference to a [Column], a [Literal] value or a [Function] invocation.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Expression {
    /**
     * A [Function] expression.
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

    /**
     * A [Literal] value expression.
     */
    @JvmInline
    value class Literal(val value: Any): Expression {
        override fun toGrpc(): CottontailGrpc.Expression {
            val expression = CottontailGrpc.Expression.newBuilder()
            expression.literal = this.value.toGrpc()
            return expression.build()
        }
    }

    /**
     * A [Column] expression.
     */
    @JvmInline
    value class Column(val name: String): Expression {
        override fun toGrpc(): CottontailGrpc.Expression = CottontailGrpc.Expression.newBuilder().setColumn(name.parseColumn()).build()
    }

    /**
     * Converts this [Expression] into a [CottontailGrpc.Expression]
     */
    fun toGrpc(): CottontailGrpc.Expression
}
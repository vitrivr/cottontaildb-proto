package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseFunction
import org.vitrivr.cottontail.client.language.extensions.toLiteral
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
        @Suppress("CAST_NEVER_SUCCEEDS")
        override fun toGrpc(): CottontailGrpc.Expression = CottontailGrpc.Expression.newBuilder().setLiteral(
            when(this.value) {
                is Array<*> -> {
                    require(this.value[0] is Number) { "Only arrays of numbers can be converted to vector literals." }
                    (this as Array<Number>).toLiteral()
                }
                is BooleanArray -> this.value.toLiteral()
                is IntArray -> this.value.toLiteral()
                is LongArray -> this.value.toLiteral()
                is FloatArray -> this.value.toLiteral()
                is DoubleArray -> this.value.toLiteral()
                is Boolean -> this.value.toLiteral()
                is Byte -> this.value.toLiteral()
                is Short -> this.value.toLiteral()
                is Int -> this.value.toLiteral()
                is Long -> this.value.toLiteral()
                is Float -> this.value.toLiteral()
                is Double -> this.value.toLiteral()
                is String -> this.value.toLiteral()
                else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to literal is not supported.")
            }).build()
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
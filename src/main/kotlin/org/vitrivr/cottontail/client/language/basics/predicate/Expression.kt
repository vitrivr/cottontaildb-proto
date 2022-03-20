package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.client.language.basics.Expression
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseNot
import org.vitrivr.cottontail.client.language.extensions.parseOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Expression] [Atomic] [Predicate] involving the evaluation of an [Expression].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class Expression(val left: String, val operator: String, val expressions: List<Expression> = emptyList(), val not: Boolean = operator.parseNot()): Atomic {

    companion object {
        /**
         * Converts the given [Any] into a [List] of [Expression.Literal].
         *
         * This is a helper function to construct an [Expression] [Predicate] from simple value arguments.
         */
        private fun convertValue(value: Any): List<Expression.Literal> = when(value) {
            is Array<*> -> value.map {
                require(it != null) { "Cannot convert null value to literal expression." }
                Expression.Literal(it)
            }
            is Iterable<*> -> value.map {
                require(it != null) { "Cannot convert null value to literal expression." }
                Expression.Literal(it)
            }
            else -> listOf( Expression.Literal(this))
        }
    }

    constructor(column: String, operator: String, value: Any) : this(column, operator, convertValue(value))
    override fun toGrpc(): CottontailGrpc.AtomicBooleanPredicate.Builder = CottontailGrpc.AtomicBooleanPredicate.newBuilder()
        .setLeft(this.left.parseColumn())
        .setOp(this.operator.parseOperator())
        .setNot(this.not)
        .setRight(
            CottontailGrpc.AtomicBooleanOperand.newBuilder().setExpressions(
            CottontailGrpc.Expressions.newBuilder().addAllExpression(this.expressions.map { exp -> exp.toGrpc() }))
        )
}
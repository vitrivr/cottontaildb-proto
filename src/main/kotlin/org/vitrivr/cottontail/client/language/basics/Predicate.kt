package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseNot
import org.vitrivr.cottontail.client.language.extensions.parseOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A simple [Predicate] used in a [Query].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed interface Predicate {
    /**
     * A simple [Compound]
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    sealed class Compound(val left: Predicate, val right: Predicate): Predicate {
        protected abstract val operator: CottontailGrpc.ConnectionOperator
        fun toGrpc(): CottontailGrpc.CompoundBooleanPredicate.Builder {
            val builder = CottontailGrpc.CompoundBooleanPredicate.newBuilder().setOp(this.operator)
            when(this.left) {
                is And -> builder.setCleft(this.left.toGrpc())
                is Or -> builder.setCleft(this.left.toGrpc())
                is Atomic -> builder.setAleft(this.left.toGrpc())
            }
            when(this.right) {
                is And -> builder.setCright(this.right.toGrpc())
                is Or -> builder.setCright(this.right.toGrpc())
                is Atomic -> builder.setAright(this.right.toGrpc())
            }
            return builder
        }
    }

    /**
     * An [And] [Compound]
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class And(left: Predicate, right: Predicate): Compound(left, right) {
        override val operator = CottontailGrpc.ConnectionOperator.AND
    }

    /**
     * An [Or] [Compound]
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    class Or(left: Predicate, right: Predicate): Compound(left, right) {
        override val operator = CottontailGrpc.ConnectionOperator.OR
    }

    /**
     * An [Atomic] [Predicate]
     *
     * @author Ralph Gasser
     * @version 1.0.0
     */
    sealed interface Atomic: Predicate {
        /**
         * Converts this [Predicate.Atomic] to a gRPC representation.
         */
        fun toGrpc(): CottontailGrpc.AtomicBooleanPredicate.Builder

        /**
         * A [Simple] [Atomic] [Predicate] involving the evaluation of an [Expression].
         *
         * @author Ralph Gasser
         * @version 1.1.0
         */
        class Simple(val left: String, val operator: String, val expressions: List<Expression> = emptyList(), val not: Boolean = operator.parseNot()): Atomic {
            constructor(column: String, operator: String, value: Any) : this(column, operator, listOf(Expression.Literal(value)))
            override fun toGrpc(): CottontailGrpc.AtomicBooleanPredicate.Builder = CottontailGrpc.AtomicBooleanPredicate.newBuilder()
                .setLeft(this.left.parseColumn())
                .setOp(this.operator.parseOperator())
                .setNot(this.not)
                .setRight(CottontailGrpc.AtomicBooleanOperand.newBuilder().setExpressions(
                    CottontailGrpc.Expressions.newBuilder().addAllExpression(this.expressions.map { exp -> exp.toGrpc() }))
                )
        }

        /**
         * A [SubSelect] [Atomic] [Predicate] involving the evaluation of a sub-select [Query]
         *
         * @author Ralph Gasser
         * @version 1.1.0
         */
        class SubSelect(val left: String, val operator: String, val right: Query, val not: Boolean = operator.parseNot()): Atomic {
            override fun toGrpc(): CottontailGrpc.AtomicBooleanPredicate.Builder = CottontailGrpc.AtomicBooleanPredicate.newBuilder()
                .setLeft(this.left.parseColumn())
                .setOp(this.operator.parseOperator())
                .setNot(this.not)
                .setRight(CottontailGrpc.AtomicBooleanOperand.newBuilder().setQuery(this.right.builder.queryBuilder))
        }
    }
}
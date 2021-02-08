package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A simple [Predicate]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class Predicate

/**
 * A simple [CompoundBooleanPredicate]
 *
 * @author Ralph Gasser
 * @version 1.0
 */
sealed class CompoundBooleanPredicate(val left: Predicate, val right: Predicate): Predicate() {
    protected abstract val operator: CottontailGrpc.ConnectionOperator
    fun toPredicate(): CottontailGrpc.CompoundBooleanPredicate.Builder {
        val builder = CottontailGrpc.CompoundBooleanPredicate.newBuilder().setOp(this.operator)
        when(this.left) {
            is And -> builder.setCleft(this.left.toPredicate())
            is Atomic -> builder.setAleft(this.left.toPredicate())
            is Or -> builder.setCleft(this.left.toPredicate())
        }
        when(this.right) {
            is And -> builder.setCright(this.right.toPredicate())
            is Atomic -> builder.setAright(this.right.toPredicate())
            is Or -> builder.setCright(this.right.toPredicate())
        }
        return builder
    }
}

/**
 * A [And] [CompoundBooleanPredicate]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class And(left: Predicate, right: Predicate): CompoundBooleanPredicate(left, right) {
    override val operator = CottontailGrpc.ConnectionOperator.AND
}

/**
 * An [Or] [CompoundBooleanPredicate]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Or(left: Predicate, right: Predicate): CompoundBooleanPredicate(left, right) {
    override val operator = CottontailGrpc.ConnectionOperator.OR
}

/**
 * An [Atomic] [CompoundBooleanPredicate]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Atomic(
    val left: CottontailGrpc.ColumnName,
    val operator: CottontailGrpc.ComparisonOperator,
    val values: List<CottontailGrpc.Literal>,
    val not: Boolean = false): Predicate() {
    constructor(column: String, operator: String, vararg values: Any) : this(
        column.parseColumn(),
        operator.parseOperator(),
        values.map { it.toLiteral() }
    )
    fun toPredicate(): CottontailGrpc.AtomicLiteralBooleanPredicate.Builder
        = CottontailGrpc.AtomicLiteralBooleanPredicate.newBuilder()
            .setLeft(this.left)
            .setOp(this.operator)
            .setNot(this.not)
            .addAllRight(this.values)
}
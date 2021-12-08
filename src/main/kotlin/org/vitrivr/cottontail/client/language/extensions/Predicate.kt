package org.vitrivr.cottontail.client.language.extensions

import org.vitrivr.cottontail.client.language.dql.Query
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
            is Or -> builder.setCleft(this.left.toPredicate())
            is Atomic -> builder.setAleft(this.left.toPredicate())
        }
        when(this.right) {
            is And -> builder.setCright(this.right.toPredicate())
            is Or -> builder.setCright(this.right.toPredicate())
            is Atomic -> builder.setAright(this.right.toPredicate())
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
 * An [Atomic] [Predicate]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
sealed class Atomic: Predicate() {
    abstract fun toPredicate(): CottontailGrpc.AtomicBooleanPredicate.Builder
}

/**
 * A [Literal] [Atomic] [Predicate]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Literal(val left: CottontailGrpc.ColumnName, val operator: CottontailGrpc.ComparisonOperator, val values: List<CottontailGrpc.Literal>, val not: Boolean = false): Atomic() {
    constructor(column: String, operator: String, vararg values: Any) : this(column.parseColumn(), operator.parseOperator(), values.map { it.convert() }, operator.parseNot())

    override fun toPredicate(): CottontailGrpc.AtomicBooleanPredicate.Builder = CottontailGrpc.AtomicBooleanPredicate.newBuilder()
        .setLeft(this.left)
        .setOp(this.operator)
        .setNot(this.not)
        .setRight(CottontailGrpc.AtomicBooleanOperand.newBuilder().setExpressions(
            CottontailGrpc.Expressions.newBuilder().addAllExpression(this.values.map { v -> CottontailGrpc.Expression.newBuilder().setLiteral(v).build() }))
        )
}

/**
 * A [Reference] [Atomic] [Predicate]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Reference(val left: CottontailGrpc.ColumnName, val operator: CottontailGrpc.ComparisonOperator, val right: CottontailGrpc.ColumnName, val not: Boolean = false): Atomic() {
    constructor(left: String, operator: String, right: String) : this(left.parseColumn(), operator.parseOperator(), right.parseColumn())
    override fun toPredicate(): CottontailGrpc.AtomicBooleanPredicate.Builder = CottontailGrpc.AtomicBooleanPredicate.newBuilder()
        .setLeft(this.left)
        .setOp(this.operator)
        .setNot(this.not)
        .setRight(
            CottontailGrpc.AtomicBooleanOperand.newBuilder().setExpressions(CottontailGrpc.Expressions.newBuilder().addExpression(
                CottontailGrpc.Expression.newBuilder().setColumn(this.right))
            )
        )
}

/**
 * A [SubSelect] [Atomic] [Predicate]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SubSelect(val left: CottontailGrpc.ColumnName, val operator: CottontailGrpc.ComparisonOperator, val right: Query, val not: Boolean = false): Atomic() {
    constructor(column: String, operator: String, query: Query) : this(column.parseColumn(), operator.parseOperator(), query)
    override  fun toPredicate(): CottontailGrpc.AtomicBooleanPredicate.Builder = CottontailGrpc.AtomicBooleanPredicate.newBuilder()
        .setLeft(this.left)
        .setOp(this.operator)
        .setNot(this.not)
        .setRight(CottontailGrpc.AtomicBooleanOperand.newBuilder().setQuery(this.right.builder.queryBuilder))
}

/**
 * Converts an [Any] to a [CottontailGrpc.Literal]
 *
 * @return [CottontailGrpc.Literal]
 */
@Suppress("UNCHECKED_CAST")
private fun Any.convert(): CottontailGrpc.Literal = when(this) {
    is Array<*> -> {
        require(this[0] is Number) { "Only arrays of numbers can be converted to vector literals." }
        (this as Array<Number>).toLiteral()
    }
    is BooleanArray -> this.toLiteral()
    is IntArray -> this.toLiteral()
    is LongArray -> this.toLiteral()
    is FloatArray -> this.toLiteral()
    is DoubleArray -> this.toLiteral()
    is Boolean -> this.toLiteral()
    is Byte -> this.toLiteral()
    is Short -> this.toLiteral()
    is Int -> this.toLiteral()
    is Long -> this.toLiteral()
    is Float -> this.toLiteral()
    is Double -> this.toLiteral()
    is String -> this.toLiteral()
    else -> throw IllegalStateException("Conversion of ${this.javaClass.simpleName} to literal is not supported.")
}
package org.vitrivr.cottontail.client.language.basics.predicate

import kotlinx.serialization.SerialName
import kotlinx.serialization.Serializable
import org.vitrivr.cottontail.client.language.basics.expression.Expression
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [Predicate] that compares two [Expression]s using a [Compare.Operator].
 */
@Serializable
@SerialName("Compare")
data class Compare(val lexp: Expression, val operator: Operator, val rexp: Expression): Predicate() {
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setComparison(
        CottontailGrpc.Predicate.Comparison.newBuilder().setLexp(this.lexp.toGrpc()).setRexp(this.rexp.toGrpc()).setOperator(this.operator.grpc)
    ).build()

    /**
     * Enumeration of supported [Compare] operators.
     */
    @Serializable
    enum class Operator(val symbol: String, val grpc: CottontailGrpc.Predicate.Comparison.Operator) {
        EQUAL("=", CottontailGrpc.Predicate.Comparison.Operator.EQUAL),
        NOTEQUAL("!=", CottontailGrpc.Predicate.Comparison.Operator.NOTEQUAL),
        GREATER(">", CottontailGrpc.Predicate.Comparison.Operator.GREATER),
        LESS("<", CottontailGrpc.Predicate.Comparison.Operator.LESS),
        GEQUAL(">=", CottontailGrpc.Predicate.Comparison.Operator.GEQUAL),
        LEQUAL("<=", CottontailGrpc.Predicate.Comparison.Operator.LEQUAL),
        IN("IN", CottontailGrpc.Predicate.Comparison.Operator.IN),
        BETWEEN("BETWEEN", CottontailGrpc.Predicate.Comparison.Operator.BETWEEN),
        LIKE("LIKE", CottontailGrpc.Predicate.Comparison.Operator.LIKE),
        MATCH("MATCH", CottontailGrpc.Predicate.Comparison.Operator.MATCH);

        /**
         * Parses a [String] into an [Operator]
         *
         * @return [Operator]
         */
        fun parse(string: String): Operator = Operator.values().find { it.symbol == string.uppercase() } ?: throw IllegalArgumentException("The comparison operator $string cannot be parsed.")
    }
}
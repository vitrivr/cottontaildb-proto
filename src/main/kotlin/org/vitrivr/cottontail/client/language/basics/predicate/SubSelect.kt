package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.extensions.parseColumn
import org.vitrivr.cottontail.client.language.extensions.parseNot
import org.vitrivr.cottontail.client.language.extensions.parseOperator
import org.vitrivr.cottontail.grpc.CottontailGrpc

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
package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.client.language.extensions.toGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A simple [Predicate] that consists of two sub [Predicate]s connected by an operator.
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
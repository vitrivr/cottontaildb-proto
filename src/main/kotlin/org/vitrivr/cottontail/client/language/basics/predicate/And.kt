package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A boolean AND operator, which can be used as [Predicate].
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
data class And(val left: Predicate, val right: Predicate): Predicate {
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setAnd(
        CottontailGrpc.Predicate.And.newBuilder().setLeft(this.left.toGrpc()).setRight(this.right.toGrpc())
    ).build()
}
package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A boolean OR operator, which can be used as [Predicate].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Or(val left: Predicate, val right: Predicate): Predicate{
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setOr(
        CottontailGrpc.Predicate.Or.newBuilder().setLeft(this.left.toGrpc()).setRight(this.right.toGrpc())
    ).build()
}
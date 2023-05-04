package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A boolean NOT operator, which can be used as [Predicate].
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class Not(val p: Predicate): Predicate {
    override fun toGrpc(): CottontailGrpc.Predicate = CottontailGrpc.Predicate.newBuilder().setNot(
        CottontailGrpc.Predicate.Not.newBuilder().setP(this.p.toGrpc())
    ).build()
}
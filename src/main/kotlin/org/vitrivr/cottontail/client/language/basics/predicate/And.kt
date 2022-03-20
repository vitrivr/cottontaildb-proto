package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An [And] [Compound]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class And(left: Predicate, right: Predicate): Compound(left, right) {
    override val operator = CottontailGrpc.ConnectionOperator.AND
}
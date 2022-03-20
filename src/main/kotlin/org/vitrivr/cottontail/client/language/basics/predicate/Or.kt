package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * An [Or] [Compound]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class Or(left: Predicate, right: Predicate): Compound(left, right) {
    override val operator = CottontailGrpc.ConnectionOperator.OR
}
package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.client.language.basics.Expression
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
* A [Predicate] that consists of a simple comparison.
*
* @author Ralph Gasser
* @version 1.0.0
*/
sealed interface Atomic: Predicate {
    /**
     * Converts this [Atomic] to a gRPC representation.
     */
    fun toGrpc(): CottontailGrpc.AtomicBooleanPredicate.Builder
}

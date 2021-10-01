package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * Order directions available to Cottontail DB.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
enum class Direction {
    ASC,
    DESC;


    /**
     * Converts this [Direction] to its gGRPC representation.
     */
    fun toGrpc() = when(this) {
        ASC -> CottontailGrpc.Order.Direction.ASCENDING
        DESC -> CottontailGrpc.Order.Direction.DESCENDING
    }
}
package org.vitrivr.cottontail.client.language.ddl

import org.vitrivr.cottontail.client.language.extensions.parseIndex
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A DROP INDEX query in the Cottontail DB query language.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class DropIndex(name: String) {
    val builder = CottontailGrpc.DropIndexMessage.newBuilder()

    init {
        this.builder.index = name.parseIndex()
    }
}
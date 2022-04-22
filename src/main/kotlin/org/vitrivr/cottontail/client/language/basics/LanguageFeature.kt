package org.vitrivr.cottontail.client.language.basics

import org.vitrivr.cottontail.client.language.extensions.toGrpc
import org.vitrivr.cottontail.grpc.CottontailGrpc

/**
 * A [LanguageFeature] provided by the Cottontail DB simple API.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
abstract class LanguageFeature {
    /**
     * Sets the transaction ID for this [LanguageFeature].
     *
     * @param txId The new transaction ID.
     */
    abstract fun txId(txId: Long): LanguageFeature

    /**
     * Sets the query ID for this [LanguageFeature].
     *
     * @param queryId The new query ID.
     */
    abstract fun queryId(queryId: String): LanguageFeature
}
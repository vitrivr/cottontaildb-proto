package org.vitrivr.cottontail.client.iterators

/**
 * An [Iterator] for [Tuple]s as returned by the [org.vitrivr.cottontail.client.SimpleClient]
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
interface TupleIterator : Iterator<Tuple>, AutoCloseable {
    /** The ID of the Cottontail DB transaction this [TupleIterator] is associated with. */
    val transactionId: Long

    /** The ID of the Cottontail DB query this [TupleIterator] is associated with. */
    val queryId: String

    /** Number of columns returned by this [TupleIterator]. */
    val numberOfColumns: Int

    /** [List] of column names returned by this [TupleIterator] in order of occurrence. Contains fully qualified names. */
    val columns: List<String>

    /**
     * [List] of column names returned by this [TupleIterator] in order of occurrence. Contains simple names.
     *
     * Since simple names may collide, list may be incomplete for given query.
     */
    val simple: List<String>
}
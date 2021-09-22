package org.vitrivr.cottontail.client.iterators

/**
 * An [Iterator] for [Tuple]s as returned by the [org.vitrivr.cottontail.client.SimpleClient]
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
interface TupleIterator : Iterator<Tuple>, AutoCloseable {
    /** Returns true if this [TupleIterator] is done fetching messages. */
    val completed: Boolean

    /** Number of columns returned by this [TupleIterator]. */
    val numberOfColumns: Int

    /** Columns returned by this [TupleIterator] in order of occurrence. */
    val columns: Collection<String>
}
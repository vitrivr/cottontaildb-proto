package org.vitrivr.cottontail.client.iterators

import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*

/**
 * A [TupleIterator] used for retrieving [Tuple]s in a synchronous fashion.
 *
 * Usually used with unary, server-side calls that only return a limited amount of data.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class TupleIteratorImpl internal constructor(
    private val results: Iterator<CottontailGrpc.QueryResponseMessage>,
    private val finalizer: ((TupleIterator, Boolean) -> Unit)
) : TupleIterator {

    /** Constructor for single [CottontailGrpc.QueryResponseMessage]. */
    constructor(result: CottontailGrpc.QueryResponseMessage, onComplete: ((TupleIterator, Boolean) -> Unit)): this(sequenceOf(result).iterator(), onComplete)

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private val buffer = LinkedList<Tuple>()

    /** Internal map of columns names to column indexes. */
    private val _columns = LinkedHashMap<String,Int>()

    /** Internal map of simple names to column indexes. */
    private val _simple = LinkedHashMap<String,Int>()

    /** The ID of the Cottontail DB transaction this [TupleIterator] is associated with. */
    override val transactionId: Long

    /** The ID of the Cottontail DB query this [TupleIterator] is associated with. */
    override val queryId: String

    /** Returns the columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val columns: List<String>
        get() = this._columns.keys.toList()

    /**
     *  [List] of column names returned by this [TupleIterator] in order of occurrence. Contains simple names.
     *
     *  Since simple names may collide, list may be incomplete for given query.
     */
    override val simple: List<String>
        get() = this._simple.keys.toList()

    /** Returns the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val numberOfColumns: Int
        get() = this.columns.size

    init {
        /* Start loading first results. */
        val next = this.results.next()

        /* Assign metadata, columns and data. */
        this.transactionId = next.metadata.transactionId
        this.queryId = next.metadata.queryId
        next.tuplesList.forEach { this.buffer.add(TupleImpl(it)) }
        next.columnsList.forEachIndexed { i,c ->
            this._columns[c.fqn()] = i
            if (!this._simple.contains(c.name)) {
                this._simple[c.name] = i /* If a simple name is not unique, only the first occurrence is returned. */
            }
        }

        /** Call finalizer if no more data is available. */
        if (!this.results.hasNext()) {
            this.finalizer.invoke(this, false)
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean {
        if (this.buffer.isNotEmpty()) return true
        if (!this.results.hasNext()) {
            this.finalizer.invoke(this, false)
            return false
        }
        return true
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple {
        if (this.buffer.isEmpty()) {
            if (!this.results.hasNext()) {
                this.finalizer.invoke(this, false)
                throw IllegalArgumentException("TupleIterator has been drained and no more elements can be loaded. Call hasNext() to ensure that elements are available before calling next().")
            }
            this.results.next().tuplesList.forEach { this.buffer.add(TupleImpl(it)) }
        }
        return this.buffer.poll()!!
    }

    /**
     * Closes this [TupleIteratorImpl].
     */
    override fun close() {
        this.finalizer.invoke(this, true)
    }

    inner class TupleImpl(tuple: CottontailGrpc.QueryResponseMessage.Tuple): Tuple(tuple) {
        override fun indexForName(name: String) = (this@TupleIteratorImpl._columns[name] ?: this@TupleIteratorImpl._simple[name]) ?: throw IllegalArgumentException("Column $name not known to this TupleIterator.")
        override fun asBoolean(name: String) = asBoolean(indexForName(name))
        override fun asInt(name: String) = asInt(indexForName(name))
        override fun asLong(name: String) = asLong(indexForName(name))
        override fun asFloat(name: String) = asFloat(indexForName(name))
        override fun asDouble(name: String) = asDouble(indexForName(name))
        override fun asBooleanVector(name: String) = asBooleanVector(indexForName(name))
        override fun asIntVector(name: String) = asIntVector(indexForName(name))
        override fun asLongVector(name: String) = asLongVector(indexForName(name))
        override fun asFloatVector(name: String) = asFloatVector(indexForName(name))
        override fun asDoubleVector(name: String) = asDoubleVector(indexForName(name))
        override fun asDate(name: String) = asDate(indexForName(name))
        override fun asString(name: String) = asString(indexForName(name))
    }
}
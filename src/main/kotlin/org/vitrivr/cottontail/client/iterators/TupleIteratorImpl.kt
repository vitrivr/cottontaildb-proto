package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import io.grpc.Status
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
class TupleIteratorImpl(private val results: Iterator<CottontailGrpc.QueryResponseMessage>) : TupleIterator {

    /** Constructor for single [CottontailGrpc.QueryResponseMessage]. */
    constructor(result: CottontailGrpc.QueryResponseMessage) : this(sequenceOf(result).iterator())

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private var buffer = LinkedList<CottontailGrpc.QueryResponseMessage.Tuple>()

    /** Internal map of columns names to column indexes. */
    private val _columns = LinkedHashMap<String,Int>()

    /** Internal map of simple names to column indexes. */
    private val _simple = LinkedHashMap<String,Int>()

    /** [Context.CancellableContext] to which this [TupleIterator] is bound.  */
    private val context = Context.current().withCancellation()

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

    /** False as long [Iterator] can return values. */
    override val completed: Boolean
        get() = !this.results.hasNext()

    /** Returns the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val numberOfColumns: Int
        get() = this.columns.size

    init {
        /* Start loading first results. */
        val restoreTo = this.context.attach()
        var close = true
        try {
            if (this.results.hasNext()) {
                /* Fetch data. */
                val next = this.results.next()
                this.buffer.addAll(next.tuplesList)

                /* Prepare column data. */
                next.columnsList.forEachIndexed { i,c ->
                    this._columns[c.fqn()] = i
                    if (!this._simple.contains(c.name)) {
                        this._simple[c.name] = i /* If a simple name is not unique, only the first occurrence is returned. */
                    }
                }
                close = false
            }
        } finally {
            if (close) {
                this.context.detachAndCancel(restoreTo, null)
            } else {
                this.context.detach(restoreTo)
            }
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean {
        val restoreTo = this.context.attach()
        var close = false
        try {
            if (this.buffer.isNotEmpty()) return true
            if (this.context.isCancelled) return false
            if (!this.results.hasNext()) {
                close = true
                return false
            }
            return true
        } finally {
            if (close) {
                this.context.detachAndCancel(restoreTo, null)
            } else {
                this.context.detach(restoreTo)
            }
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple {
        val restoreTo = this.context.attach()
        var close = true
        try {
            if (this.buffer.isEmpty()) {
                check(!this.context.isCancelled) { "TupleIterator has been drained and closed. Call hasNext() to ensure that elements are available before calling next()." }
                if (this.results.hasNext()) {
                    this.buffer.addAll(this.results.next().tuplesList)
                } else {
                    close = true
                    throw IllegalArgumentException("TupleIterator has been drained and no more elements can be loaded. Call hasNext() to ensure that elements are available before calling next().")
                }
            }
            return TupleImpl(this.buffer.poll()!!)
        } finally {
            if (close) {
                this.context.detachAndCancel(restoreTo, null)
            } else {
                this.context.detach(restoreTo)
            }
        }
    }

    /**
     * Closes this [TupleIteratorImpl].
     */
    override fun close() {
        if (!this.context.isCancelled) {
            this.context.cancel(Status.CANCELLED.withDescription("TupleIterator was prematurely closed by the user.").asException())
        }
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
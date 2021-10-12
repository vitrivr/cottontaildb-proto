package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import io.grpc.Status
import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import kotlin.collections.HashMap

/**
 * A [TupleIterator] used for retrieving [Tuple]s in a synchronous fashion.
 *
 * Usually used with unary, server-side calls that only return a limited amount of data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class SynchronousTupleIterator(private val results: Iterator<CottontailGrpc.QueryResponseMessage>, val context: Context.CancellableContext) : TupleIterator {

    /** Constructor for single [CottontailGrpc.QueryResponseMessage]. */
    constructor(result: CottontailGrpc.QueryResponseMessage, context: Context.CancellableContext) : this(sequenceOf(result).iterator(), context)

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private var buffer = LinkedList<CottontailGrpc.QueryResponseMessage.Tuple>()

    /** Internal map of columns names to column indexes. */
    private val _columns = HashMap<String,Int>()

    /** Internal map of simple names to column indexes. */
    private val _simple = HashMap<String,Int>()

    /** The [Context.CancellableContext] in which the query processed by this [SynchronousTupleIterator] gets executed. */
    private val localContext = context.withCancellation()

    /** Returns the columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val columns: Collection<String>
        get() = Collections.unmodifiableCollection(this._columns.keys)

    /** False as long [Iterator] can return values. */
    override val completed: Boolean
        get() = !this.results.hasNext()

    /** Returns the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val numberOfColumns: Int

    init {
        /* Start loading first results. */
        if (this.results.hasNext()) {
            /* Fetch data. */
            val next = this.results.next()
            this.buffer.addAll(next.tuplesList)

            /* Prepare column data. */
            this.numberOfColumns = next.columnsCount
            next.columnsList.forEachIndexed { i,c ->
                this._columns[c.fqn()] = i
                if (!this._simple.contains(c.name)) {
                    this._simple[c.name] = i /* If a simple name is not unique, only the first occurrence is returned. */
                }
            }
        } else {
            /* Case: Empty resultset. */
            this.numberOfColumns = 0
            this.localContext.detachAndCancel(context, null)
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean = (this.buffer.isNotEmpty() || this.results.hasNext())

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple {
        if (this.buffer.isEmpty() && !this.fetchNext()) throw IllegalStateException("TupleIterator is has no more values.")
        return TupleImpl(this.buffer.poll()!!)
    }

    /**
     * Closes this [SynchronousTupleIterator].
     */
    override fun close() {
        this.localContext.detachAndCancel(context, Status.CANCELLED.withDescription("TupleIterator was prematurely closed by the user.").asException())
    }

    /**
     * Fetches the next batch of [CottontailGrpc.QueryResponseMessage] into the [buffer].
     *
     * @return True on success, false otherwise.
     */
    private fun fetchNext(): Boolean = if (this.results.hasNext()) {
        this.buffer.addAll(this.results.next().tuplesList)
        true
    } else {
        this.localContext.detachAndCancel(context, null)
        false
    }

    inner class TupleImpl(tuple: CottontailGrpc.QueryResponseMessage.Tuple): Tuple(tuple) {
        override fun indexForName(name: String) = (this@SynchronousTupleIterator._columns[name] ?: this@SynchronousTupleIterator._simple[name]) ?: throw IllegalArgumentException("Column $name not known to this TupleIterator.")
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
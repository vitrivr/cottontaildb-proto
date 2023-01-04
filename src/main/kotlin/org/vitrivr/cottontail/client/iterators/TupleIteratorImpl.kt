package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import org.vitrivr.cottontail.client.language.basics.Type
import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import java.util.concurrent.CancellationException
import kotlin.collections.ArrayList

/**
 * A [TupleIterator] used for retrieving [Tuple]s in a synchronous fashion.
 *
 * Usually used with unary, server-side calls that only return a limited amount of data.
 *
 * @author Ralph Gasser
 * @version 1.1.1
 */
class TupleIteratorImpl internal constructor(private val results: Iterator<CottontailGrpc.QueryResponseMessage>, private val context: Context.CancellableContext) : TupleIterator {

    /** Constructor for single [CottontailGrpc.QueryResponseMessage]. */
    constructor(result: CottontailGrpc.QueryResponseMessage, context: Context.CancellableContext): this(sequenceOf(result).iterator(), context)

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

    /** The time it took in milliseconds to plan the query. */
    override val planDuration: Long

    /** The time it took in milliseconds to execute the query. */
    override val queryDuration: Long

    /** The column names returned by this [TupleIterator]. */
    override val columnNames: List<String> = ArrayList()

    /** [List] of simple column names returned by this [TupleIterator] in order of occurrence. */
    override val simpleNames: List<String> = ArrayList()

    /** The column [Type]s returned by this [TupleIterator]. */
    override val columnTypes: List<Type> = ArrayList()

    /** Returns the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    override val numberOfColumns: Int
        get() = this.columnNames.size

    init {
        /* Start loading first results. */
        val next = this.results.next()

        /* Assign metadata, columns and data. */
        this.transactionId = next.metadata.transactionId
        this.queryId = next.metadata.queryId
        this.planDuration = next.metadata.planDuration
        this.queryDuration = next.metadata.queryDuration

        next.tuplesList.forEach { this.buffer.add(TupleImpl(it)) }
        next.columnsList.forEachIndexed { i,c ->
            this._columns[c.name.fqn()] = i
            (this.columnNames as MutableList).add(c.name.fqn())
            (this.simpleNames as MutableList).add(c.name.name)
            (this.columnTypes as MutableList).add(Type.of(c.type))
            if (!this._simple.contains(c.name.name)) {
                this._simple[c.name.name] = i /* If a simple name is not unique, only the first occurrence is returned. */
            }
        }

        /** Call finalizer if no more data is available. */
        if (!this.results.hasNext()) {
            this.context.close()
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean {
        if (this.buffer.isNotEmpty()) return true
        if (!this.results.hasNext()) {
            this.context.close()
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
                /* Should never be reached. */
                this.context.close()
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
        this.context.cancel(CancellationException("TupleIterator has been prematurely closed by user."))
    }

    inner class TupleImpl(tuple: CottontailGrpc.QueryResponseMessage.Tuple): Tuple(tuple) {
        override fun nameForIndex(index: Int): String = this@TupleIteratorImpl.columnNames[index]
        override fun simpleNameForIndex(index: Int): String = this@TupleIteratorImpl.simpleNames[index]
        override fun indexForName(name: String) = (this@TupleIteratorImpl._columns[name] ?: this@TupleIteratorImpl._simple[name]) ?: throw IllegalArgumentException("Column $name not known to this TupleIterator.")
        override fun type(index: Int): Type = this@TupleIteratorImpl.columnTypes[index]
        override fun type(name: String): Type = this@TupleIteratorImpl.columnTypes[indexForName(name)]
        override fun asBoolean(name: String) = asBoolean(indexForName(name))
        override fun asByte(name: String) = asByte(indexForName(name))
        override fun asShort(name: String) = asShort(indexForName(name))
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
        override fun asByteString(name: String): ByteArray? = asByteString(indexForName(name))
    }
}
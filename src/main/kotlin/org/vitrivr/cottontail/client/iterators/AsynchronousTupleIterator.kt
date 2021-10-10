package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc

import java.util.*
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.HashMap
import kotlin.concurrent.withLock
import java.util.concurrent.CancellationException
import kotlin.collections.LinkedHashMap

/**
 * A very simple utility class that wraps [CottontailGrpc.QueryResponseMessage] and provides more convenient means of access.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AsynchronousTupleIterator(private val bufferSize: Int = 100): TupleIterator, StreamObserver<CottontailGrpc.QueryResponseMessage> {

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private var buffer = LinkedList<Tuple>()

    /** Internal map of columns names to column indexes. */
    private val _columns = LinkedHashMap<String,Int>()

    /** Internal lock used to synchronise access to buffer. */
    private val lock = ReentrantLock()

    /** A [Condition] used to signal, that the buffer is empty. */
    private val waitingForData: Condition = this.lock.newCondition()

    /** A [Condition] used to signal, that the buffer is full. */
    private val waitingForSpace: Condition = this.lock.newCondition()

    /** The [Context.CancellableContext] in which the query processed by this [AsynchronousTupleIterator] gets executed. */
    internal val context: Context.CancellableContext = Context.current().withCancellation()

    /** Internal flag indicating, that this [AsynchronousTupleIterator] has completed (i.e. no more [Tuple]s will be returned) */
    @Volatile
    private var error: Throwable? = null

    /** Internal flag indicating, that this [AsynchronousTupleIterator] has completed (i.e. no more [Tuple]s will be returned) */
    @Volatile
    override var completed: Boolean = false
        private set

    /** Flag indicating, that this [AsynchronousTupleIterator] has been initialized. */
    @Volatile
    var started: Boolean = false

    /** Returns the columns contained in the [Tuple]s returned by this [AsynchronousTupleIterator]. */
    override val columns: Collection<String>
        get() = this.lock.withLock {
            if (!this.started) this.waitingForData.await()
            Collections.unmodifiableCollection(this._columns.keys)
        }

    /** Number of columns contained in the [Tuple]s returned by this [AsynchronousTupleIterator]. */
    override var numberOfColumns: Int = 0
        get() = this.lock.withLock {
            if (!this.started) this.waitingForData.await()
            field
        }
        private set

    /**
     * gRPC method: Called when another [CottontailGrpc.QueryResponseMessage] is available.
     */
    override fun onNext(value: CottontailGrpc.QueryResponseMessage) = this.lock.withLock {
        /* Update columns for this TupleIterator. */
        if (!this.started) {
            this.numberOfColumns = value.columnsCount
            value.columnsList.forEachIndexed { i,c ->
                this._columns[c.fqn()] = i
            }
            this.started = true
        }

        /* Buffer tuples. This part may block. */
        for (tuple in value.tuplesList) {
            if (this.buffer.size >= this.bufferSize) {
                this.waitingForSpace.await()
            }
            this.buffer.offer(TupleImpl(tuple))
            this.waitingForData.signalAll()
        }
    }

    /**
     * gRPC method: Called when the server side reports an error.
     */
    override fun onError(t: Throwable?) = this.lock.withLock {
        /* If query hasn't started yet, mark it as started and signal (for results that produce errors). */
        if (!this.started) {
            this.started = true
        }

        /* Mark query as completed and signal completeness! */
        this.completed = true
        this.error = t
        this.context.cancel(null)

        /* Signal that new data has become available. */
        this.waitingForData.signalAll()
    }

    /**
     * gRPC method: Called when the server side completes.
     */
    override fun onCompleted() = this.lock.withLock {
        /* If query hasn't started yet, mark it as started and signal (for empty results). */
        if (!this.started) {
            this.started = true
        }

        /* Mark query as completed and signal completeness! */
        this.completed = true
        this.context.cancel(null)

        /* Signal that new data has become available. */
        this.waitingForData.signalAll()
    }

    /**
     * Returns true if this [AsynchronousTupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean = this.lock.withLock {
        /* Wait here if no data has been received yet. */
        if (!this.started) this.waitingForData.await()

        /* Now check for available data. */
        do {
            when {
                this.error != null -> throw this.error!!            /* Case Error ocurred: Throw it. */
                this.buffer.isNotEmpty() ->  return@withLock true   /* Case Buffer has data: Return true. */
                this.completed -> return@withLock false             /* Case Data loading has completed (AND buffer is empty): Return false. */
                else -> this.waitingForData.await()                 /* Case Else: Wait for more data. */
            }
        } while (true)
        false
    }

    /**
     * Returns true if this [AsynchronousTupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple = this.lock.withLock {
        /* Wait here if no data has been received yet. */
        if (!this.started) this.waitingForData.await()

        /* Poll for tuple to become available. */
        var next: Tuple?
        do {
            if (this.error != null) throw this.error!!
            if (this.completed && this.buffer.isEmpty()) throw IllegalStateException("This TupleIterator has been drained and no new elements are to be expected! It is recommended to check if new elements available using hasNext() before a call to next().")
            next = this.buffer.poll()
        } while (next == null)

        /* Signal that buffer has space again and return data. */
        this.waitingForSpace.signalAll()
        return next
    }

    /**
     * Closes this [AsynchronousTupleIterator].
     *
     * Closing a [AsynchronousTupleIterator] for a query that has not completed (i.e. whose responses have not been drained) may leads to
     * undefined behaviour on the server side and to a transaction that must be rolled back.
     */
    override fun close() {
        if (!this.completed) {
            this.context.cancel(CancellationException("TupleIterator was prematurely closed by the user."))
            this.buffer.clear()
            this.completed = true
        } else {
            this.context.cancel(null)
        }
    }

    inner class TupleImpl(tuple: CottontailGrpc.QueryResponseMessage.Tuple): Tuple(tuple) {
        override operator fun get(name: String) = get(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asBoolean(name: String) = asBoolean(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asInt(name: String) = asInt(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asLong(name: String) = asLong(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asFloat(name: String) = asFloat(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asDouble(name: String) = asDouble(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asBooleanVector(name: String) = asBooleanVector(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asIntVector(name: String) = asIntVector(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asLongVector(name: String) = asLongVector(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asFloatVector(name: String) = asFloatVector(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asDoubleVector(name: String) = asDoubleVector(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asDate(name: String) = asDate(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
        override fun asString(name: String) = asString(this@AsynchronousTupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
    }
}
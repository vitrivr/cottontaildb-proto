package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc

import java.util.*
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
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

    /** Internal map of simple names to column indexes. */
    private val _simple = LinkedHashMap<String,Int>()

    /** Internal lock used to synchronise access to buffer. */
    private val lock = ReentrantLock()

    /** A [Condition] used to signal, that data loading has started. */
    private val waitingForStart: Condition = this.lock.newCondition()

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
    private var started = false

    /** Flag indicating, that this [AsynchronousTupleIterator] has been closes. */
    @Volatile
    private var closed = false

    /** Returns the columns contained in the [Tuple]s returned by this [AsynchronousTupleIterator]. */
    override val columns: List<String>
        get() = this.lock.withLock {
            if (!this.started) this.waitingForStart.await()
            this._columns.keys.toList()
        }
    override val simple: List<String>
        get() = this.lock.withLock {
            if (!this.started) this.waitingForStart.await()
            this._simple.keys.toList()
        }

    /** Number of columns contained in the [Tuple]s returned by this [AsynchronousTupleIterator]. */
    override var numberOfColumns: Int = 0
        get() = this.lock.withLock {
            if (!this.started) this.waitingForStart.await()
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
                if (!this._simple.contains(c.name)) {
                    this._simple[c.name] = i /* If a simple name is not unique, only the first occurrence is returned. */
                }
            }

            /* Signal that data loading has started! */
            this.started = true
            this.waitingForStart.signalAll()
        }



        /* Buffer tuples; wait for space to become available if necessary... */
        for (tuple in value.tuplesList) {
            while (this.buffer.size >= this.bufferSize) {
                this.waitingForSpace.await()
            }
            this.buffer.offer(TupleImpl(tuple))
            this.waitingForData.signal()
        }

        /*  ...and signal that new data has become available. */
        this.waitingForData.signal()
    }

    /**
     * gRPC method: Called when the server side reports an error.
     */
    override fun onError(t: Throwable?) = this.lock.withLock {
        /* Mark query as completed and signal completeness! */
        if (!this.completed) {
            this.error = t
            this.completed = true
        }

        /* If query hasn't started yet, mark it as started and signal (for results that produce errors). */
        if (!this.started) {
            this.started = true
            this.waitingForStart.signalAll()
        }

        /* Signal that new data has become available. */
        this.waitingForData.signalAll()
        this.context.cancel(null)
        Unit
    }

    /**
     * gRPC method: Called when the server side completes.
     */
    override fun onCompleted() = this.lock.withLock {
        /* Mark query as completed and signal completeness! */
        if (!this.completed) {
            this.completed = true
            this.context.cancel(null)
        }

        /* If query hasn't started yet, mark it as started and signal (for empty results). */
        this.lock.lock()
        if (!this.started) {
            this.started = true
            this.waitingForStart.signalAll()
        }

        /* Signal that new data has become available. */
        this.waitingForData.signalAll()
        this.context.cancel(null)
        Unit
    }

    /**
     * Returns true if this [AsynchronousTupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean = this.lock.withLock {
        check(!this.closed) { "This TupleIterator has been closed and can no longer be used!" }

        /* Wait here if no data has been received yet. */
        if (!this.started) this.waitingForStart.await()

        /* Now check for available data. */
        do {
            if (this.completed) throw this.error ?: break
            if (this.buffer.isNotEmpty()) return true
            this.waitingForData.await()
        } while (true)
        false
    }

    /**
     * Returns true if this [AsynchronousTupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple = this.lock.withLock {
        check(!this.closed) { "This TupleIterator has been closed and can no longer be used!" }

        /* Wait here if no data has been received yet. */
        if (!this.started) this.waitingForStart.await()

        /* Poll for tuple to become available. */
        var next: Tuple?
        do {
            check(!(this.completed && this.buffer.isEmpty())) { throw IllegalStateException("This TupleIterator has been drained and no new elements are to be expected! It is recommended to check if new elements available using hasNext() before a call to next().") }
            if (this.error != null) throw this.error!!
            next = this.buffer.poll()
        } while (next == null)

        /* Signal that buffer has space again and return data. */
        if (this.buffer.size < this.bufferSize) this.waitingForSpace.signal()
        return next
    }

    /**
     * Closes this [AsynchronousTupleIterator].
     *
     * Closing a [AsynchronousTupleIterator] for a query that has not completed (i.e. whose responses have not been drained) may leads to
     * undefined behaviour on the server side and to a transaction that must be rolled back.
     */
    override fun close() = this.lock.withLock {
        if (!this.closed) {
            this.closed = true
            if (!this.completed) {
                this.context.cancel(CancellationException("TupleIterator was prematurely closed by the user."))
            }

            /* Clear buffer. */
            this.buffer.clear()

            /* Signal to all (potentially) waiting locks. */
            this.waitingForStart.signalAll()
            this.waitingForData.signalAll()
            this.waitingForSpace.signalAll()
        }
    }

    inner class TupleImpl(tuple: CottontailGrpc.QueryResponseMessage.Tuple): Tuple(tuple) {
        override fun indexForName(name: String) =  (this@AsynchronousTupleIterator._columns[name] ?: this@AsynchronousTupleIterator._simple[name]) ?: throw IllegalArgumentException("Column $name not known to this TupleIterator.")
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
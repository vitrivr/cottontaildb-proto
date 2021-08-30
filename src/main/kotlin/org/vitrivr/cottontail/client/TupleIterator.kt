package org.vitrivr.cottontail.client

import io.grpc.Context
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc

import java.util.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import java.util.concurrent.locks.Condition
import java.util.concurrent.locks.ReentrantLock
import kotlin.collections.HashMap
import kotlin.concurrent.withLock
import java.util.concurrent.CancellationException

/**
 * A very simple utility class that wraps [CottontailGrpc.QueryResponseMessage] and provides more convenient means of access.
 *
 * @author Ralph Gasser
 * @version 2.0.0
 */
class TupleIterator(val context: Context.CancellableContext, val bufferSize: Int = 100) :
    Iterator<TupleIterator.Tuple>, StreamObserver<CottontailGrpc.QueryResponseMessage>, AutoCloseable {

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private var buffer = LinkedList<Tuple>()

    /** Internal map of columns names to column indexes. */
    private val _columns = HashMap<String,Int>()

    /** Internal flag indicating, that this [TupleIterator] has been initialized. */
    private var _init: AtomicBoolean = AtomicBoolean(false)

    /** Internal flag indicating, that this [TupleIterator] has completed (i.e. no more [Tuple]s will be returned) */
    private var _completed: AtomicBoolean = AtomicBoolean(false)

    /** Internal flag indicating, that this [TupleIterator] has completed (i.e. no more [Tuple]s will be returned) */
    private var _error: AtomicReference<Throwable?> = AtomicReference(null)

    /** Internal counter for the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    private var _numberOfColumns: AtomicInteger = AtomicInteger(0)

    /** Internal lock used to synchronise access to buffer. */
    private val lock = ReentrantLock()

    /** The next [Tuple] to be dequeued. */
    private var next: Tuple? = null

    /** A [Condition] used to signal, that the buffer is not empty. */
    private val notEmpty: Condition = this.lock.newCondition()

    /** A [Condition] used to signal, that the buffer is not full. */
    private val notFull: Condition = this.lock.newCondition()

   /** Returns the columns contained in the [Tuple]s returned by this [TupleIterator]. */
    val columns: Collection<String>
        get() = Collections.unmodifiableCollection(this._columns.keys)

    /** Number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    val numberOfColumns: Int
        get() = this._numberOfColumns.get()

    /**
     * gRPC method: Called when another [CottontailGrpc.QueryResponseMessage] is available.
     */
    override fun onNext(value: CottontailGrpc.QueryResponseMessage) = this.lock.withLock {
        /* Update columns for this TupleIterator. */
        if (!this._init.getAndSet(true)) {
            this._numberOfColumns.set(value.columnsCount)
            value.columnsList.forEachIndexed { i,c ->
                this._columns[c.fqn()] = i
                if (!this._columns.contains(c.name)) {
                    this._columns[c.name] = i /* If a simple name is not unique, only the first occurrence is returned. */
                }
            }
        }

        /* Buffer tuples. This part may block. */
        for (tuple in value.tuplesList) {
            if (this.buffer.size >= this.bufferSize) {
                this.notFull.await()  /* Wait for notFull-condition. */
            }
            this.buffer.offer(Tuple(tuple))
            this.notEmpty.signal() /* Signal notEmpty.condition. */
        }
    }

    /**
     * gRPC method: Called when the server side reports an error.
     */
    override fun onError(t: Throwable?) {
        this._completed.set(true)
    }

    /**
     * gRPC method: Called when the server side completes.
     */
    override fun onCompleted() {
        this._completed.set(true)
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean {
        val error = this._error.get()
        if (error != null) {
            throw error
        }
        this.lock.withLock {
            return if (this.buffer.isNotEmpty()) {
                this.next = this.buffer.poll()
                this.notFull.signal()
                true
            } else if (this._completed.get()) {
                this.next = null
                false
            } else {
                this.notEmpty.await()
                this.next = this.buffer.poll()
                this.notFull.signal()
                true
            }
        }
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple {
        val next = this.next
        check(next != null) { "TupleIterator has been drained and now new element is available. "}
        this.next = null
        return next
    }

    /**
     * Closes this [TupleIterator].
     *
     * Closing a [TupleIterator] for a query that has not completed (i.e. whose responses have not been drained) may leads to
     * undefined behaviour on the server side and to a transaction that must be rolled back.
     */
    override fun close() {
        if (!this._completed.getAndSet(true)) {
            this.context.cancel(CancellationException("TupleIterator was prematurely closed by the user."))
        }
        this.buffer.clear() /* Clear buffer. */
    }

    /**
     * A [Tuple] as returned by the [TupleIterator].
     *
     * @author Ralph Gasser
     * @version 1.1.0
     */
    inner class Tuple(tuple: CottontailGrpc.QueryResponseMessage.Tuple) {

        /** Internal list of values. */
        private val values: Array<Any?> = Array(tuple.dataCount) { it ->
            val data = tuple.dataList[it]
            when (data.dataCase) {
                CottontailGrpc.Literal.DataCase.BOOLEANDATA -> data.booleanData
                CottontailGrpc.Literal.DataCase.INTDATA -> data.intData
                CottontailGrpc.Literal.DataCase.LONGDATA -> data.longData
                CottontailGrpc.Literal.DataCase.FLOATDATA -> data.floatData
                CottontailGrpc.Literal.DataCase.DOUBLEDATA -> data.doubleData
                CottontailGrpc.Literal.DataCase.DATEDATA -> Date(data.dateData.utcTimestamp)
                CottontailGrpc.Literal.DataCase.STRINGDATA -> data.stringData
                CottontailGrpc.Literal.DataCase.COMPLEX32DATA -> data.complex32Data.real to data.complex32Data.imaginary
                CottontailGrpc.Literal.DataCase.COMPLEX64DATA -> data.complex64Data.real to data.complex64Data.imaginary
                CottontailGrpc.Literal.DataCase.VECTORDATA -> {
                    val vector = data.vectorData
                    when (vector.vectorDataCase) {
                        CottontailGrpc.Vector.VectorDataCase.FLOATVECTOR -> FloatArray(vector.floatVector.vectorCount) { vector.floatVector.getVector(it) }
                        CottontailGrpc.Vector.VectorDataCase.DOUBLEVECTOR -> DoubleArray(vector.doubleVector.vectorCount) { vector.doubleVector.getVector(it) }
                        CottontailGrpc.Vector.VectorDataCase.INTVECTOR -> IntArray(vector.intVector.vectorCount) { vector.intVector.getVector(it) }
                        CottontailGrpc.Vector.VectorDataCase.LONGVECTOR -> LongArray(vector.longVector .vectorCount) { vector.longVector.getVector(it) }
                        CottontailGrpc.Vector.VectorDataCase.BOOLVECTOR -> BooleanArray(vector.boolVector.vectorCount) { vector.boolVector.getVector(it) }
                        CottontailGrpc.Vector.VectorDataCase.COMPLEX32VECTOR -> Array(vector.complex32Vector.vectorCount) { vector.complex32Vector.getVector(it).real to  vector.complex32Vector.getVector(it).imaginary}
                        CottontailGrpc.Vector.VectorDataCase.COMPLEX64VECTOR -> Array(vector.complex64Vector.vectorCount) { vector.complex64Vector.getVector(it).real to  vector.complex64Vector.getVector(it).imaginary}
                        else  -> UnsupportedOperationException("Vector data of type ${vector.vectorDataCase} is not supported by TupleIterator.")
                    }
                }
                CottontailGrpc.Literal.DataCase.DATA_NOT_SET -> null
                else -> UnsupportedOperationException("Data of type ${data.dataCase} is not supported by TupleIterator.")
            }
        }

        operator fun get(index: Int): Any? = this.values[index]
        operator fun get(name: String) = get(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asBoolean(index: Int): Boolean? {
            val value = this.values[index]
            return if (value is Boolean) { value } else { null }
        }
        fun asBoolean(name: String) = asBoolean(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asInt(index: Int): Int? {
            val value = this.values[index]
            return if (value is Int) { value } else { null }
        }
        fun asInt(name: String) = asInt(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asLong(index: Int): Long? {
            val value = this.values[index]
            return if (value is Long) { value } else { null }
        }
        fun asLong(name: String) = asLong(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asFloat(index: Int): Float? {
            val value = this.values[index]
            return if (value is Float) { value } else { null }
        }
        fun asFloat(name: String) = asFloat(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asDouble(index: Int): Double? {
            val value = this.values[index]
            return if (value is Double) { value } else { null }
        }
        fun asDouble(name: String) = asDouble(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asBooleanVector(index: Int): BooleanArray? {
            val value = this.values[index]
            return if (value is BooleanArray) { value } else { null }
        }
        fun asBooleanVector(name: String) = asBooleanVector(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asIntVector(index: Int): IntArray? {
            val value = this.values[index]
            return if (value is IntArray) { value } else { null }
        }
        fun asIntVector(name: String) = asIntVector(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asLongVector(index: Int): LongArray? {
            val value = this.values[index]
            return if (value is LongArray) { value } else { null }
        }
        fun asLongVector(name: String) = asLongVector(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asFloatVector(index: Int): FloatArray? {
            val value = this.values[index]
            return if (value is FloatArray) { value } else { null }
        }
        fun asFloatVector(name: String) = asFloatVector(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asDoubleVector(index: Int): DoubleArray? {
            val value = this.values[index]
            return if (value is DoubleArray) { value } else { null }
        }
        fun asDoubleVector(name: String) = asDoubleVector(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asString(index: Int): String? {
            val value = this.values[index]
            return if (value is String) { value } else { null }
        }
        fun asDate(name: String) = asString(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        fun asDate(index: Int): Date? {
            val value = this.values[index]
            return if (value is Date) { value } else { null }
        }
        fun asString(name: String) = asString(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))

        override fun toString(): String = this.values.joinToString(", ") { it?.toString() ?: "<null>" }
    }
}
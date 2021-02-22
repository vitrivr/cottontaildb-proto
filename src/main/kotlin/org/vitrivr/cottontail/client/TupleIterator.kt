package org.vitrivr.cottontail.client

import org.vitrivr.cottontail.client.language.extensions.fqn
import org.vitrivr.cottontail.grpc.CottontailGrpc

import java.util.*
import kotlin.collections.HashMap

/**
 * A very simple utility class that wraps [CottontailGrpc.QueryResponseMessage] and provides more convenient means of access.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class TupleIterator(private val results: Iterator<CottontailGrpc.QueryResponseMessage>) : Iterator<TupleIterator.Tuple> {

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private var buffer = LinkedList<CottontailGrpc.QueryResponseMessage.Tuple>()

    /** Internal buffer with pre-loaded [CottontailGrpc.QueryResponseMessage.Tuple]. */
    private val _columns = HashMap<String,Int>()

    /** Returns the columns contained in the [Tuple]s returned by this [TupleIterator]. */
    val columns: Collection<String>
        get() = Collections.unmodifiableCollection(this._columns.keys)

    /** Returns the number of columns contained in the [Tuple]s returned by this [TupleIterator]. */
    val numberOfColumns: Int
        get() = this._columns.size

    init {
        this.fetchNext()
    }

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun hasNext(): Boolean = (this.buffer.isNotEmpty() || this.results.hasNext())

    /**
     * Returns true if this [TupleIterator] holds another [Tuple] and false otherwise.
     */
    override fun next(): Tuple {
        if (this.buffer.isEmpty() && !this.fetchNext()) throw IllegalStateException("TupleIterator is has not more values.")
        return Tuple(this.buffer.poll()!!)
    }

    /**
     * Fetches the next batch of [CottontailGrpc.QueryResponseMessage] into the [buffer].
     *
     * @return True on success, false otherwise.
     */
    private fun fetchNext(): Boolean = if (this.results.hasNext()) {
        val next = this.results.next()
        if (this._columns.isEmpty()) {
            next.columnsList.forEachIndexed { i,c -> this._columns[c.fqn()] = i }
        }
        this.buffer.addAll(next.tuplesList)
        true
    } else {
        false
    }

    /**
     * A [Tuple] as returned by the [TupleIterator].
     *
     * @author Ralph Gasser
     * @version 1.0.0
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
                CottontailGrpc.Literal.DataCase.NULLDATA -> null
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
        fun asString(name: String) = asString(this@TupleIterator._columns[name] ?: throw IllegalArgumentException("Column $name not known to this TupleIterator."))
    }
}
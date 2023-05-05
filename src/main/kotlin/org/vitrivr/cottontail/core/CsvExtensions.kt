package org.vitrivr.cottontail.core


import org.vitrivr.cottontail.client.iterators.Tuple
import org.vitrivr.cottontail.core.values.*

/**
 * Converts this [Tuple] to a CSV compatible [String] representation.
 *
 * @param separator The column separator to use (defaults to a comma).
 * @param vectorSeparator The vector separator to use (default to semicolon)
 * @return [String]
 */
fun Tuple.csvEncode(separator: String = ",", vectorSeparator: String = ";") = (0..this.size()).map { this[it]?.csvEncode() }.joinToString(separator)

/**
 * Converts this [PublicValue] to a CSV compatible [String] representation.
 *
 * @return [String]
 */
fun PublicValue.csvEncode(vectorSeparator: String = ";") = when(this) {
    is BooleanValue,
    is ByteValue,
    is ShortValue,
    is IntValue,
    is LongValue,
    is DoubleValue,
    is FloatValue -> this.toString()
    is StringValue -> "\"${this}\""
    is DateValue -> this.toDate()
    is Complex32Value -> "${this.real} + i${this.imaginary}"
    is Complex64Value -> "${this.real} + i${this.imaginary}"
    is BooleanVectorValue -> encode(this)
    is DoubleVectorValue -> encode(this)
    is FloatVectorValue -> encode(this)
    is IntVectorValue -> encode(this)
    is LongVectorValue -> encode(this)
    is Complex32VectorValue -> TODO()
    is Complex64VectorValue -> TODO()
    is ByteStringValue -> "<BLOB>" /* ByteStrings cannot be exported to CSV. */
}

private fun encode(vector: BooleanVectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
private fun encode(vector: DoubleVectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
private fun encode(vector: FloatVectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
private fun encode(vector: IntVectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
private fun encode(vector: LongVectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
private fun encode(vector: Complex32VectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]") { "$"}
private fun encode(vector: Complex64VectorValue, vectorSeparator: String = ";") : String = vector.data.joinToString(separator = vectorSeparator, prefix = "[", postfix = "]")
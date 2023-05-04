package org.vitrivr.cottontail.core.values

/**
 * Represents a scalar value of any type, i.e. a value that consists only of a one entry. This is an
 * abstraction over the existing primitive array types provided by Kotlin. It allows for the advanced
 * type system implemented by Cottontail DB.
 *
 * @version 2.0.0
 * @author Ralph Gasser
 */
sealed interface ScalarValue<T: Any>: ExposedValue {
    /** Actual value of this [Value]. */
    val value: T
}
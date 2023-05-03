package org.vitrivr.cottontail.client.language.basics.predicate

import org.vitrivr.cottontail.client.language.basics.expression.Expression

/**
 * A IS NULL operator, which can be used as [Predicate] to evaluate if an [Expression] is null.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
data class IsNull(val expression: Expression)
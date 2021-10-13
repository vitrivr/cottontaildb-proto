package org.vitrivr.cottontail.client.iterators

import io.grpc.Context
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.grpc.CottontailGrpc
import java.util.*
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.atomic.AtomicBoolean

/**
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class AsynchronousTupleIterator : TupleIterator, StreamObserver<CottontailGrpc.QueryResponseMessage> {

    /** [Context.CancellableContext] to which this [TupleIterator] is bound.  */
    private val context = Context.current().withCancellation()

    /** Internal buffer of [CottontailGrpc.QueryResponseMessage]s. */
    private val next = SynchronousQueue<CottontailGrpc.QueryResponseMessage>()

    /** Internal buffer that holds [Tuple]s. */
    private val buffer = LinkedList<CottontailGrpc.QueryResponseMessage.Tuple>()

    override val completed: Boolean
        get() = TODO("Not yet implemented")

    override val numberOfColumns: Int
        get() = TODO("Not yet implemented")

    override val columns: List<String>
        get() = TODO("Not yet implemented")

    override val simple: List<String>
        get() = TODO("Not yet implemented")

    /** Internal flag indicating, that data loading has started. */
    private val started = AtomicBoolean(false)

    init {
        val first = this.next.poll()  /** Wait for first CottontailGrpc.QueryResponseMessage to become available. */
    }

    /**
     * Called by the gRPC library; enqueues the next [CottontailGrpc.QueryResponseMessage] into the [next].
     *
     * @param value The next [CottontailGrpc.QueryResponseMessage].
     */
    override fun onNext(value: CottontailGrpc.QueryResponseMessage) = this.next.put(value)

    override fun onError(t: Throwable?) {
        TODO("Not yet implemented")
    }

    override fun onCompleted() {
        TODO("Not yet implemented")
    }
    override fun hasNext(): Boolean {
        TODO("Not yet implemented")
    }

    override fun next(): Tuple {
        TODO("Not yet implemented")
    }

    override fun close() {
        TODO("Not yet implemented")
    }

    /**
     *
     */
    private fun processMessage(message: CottontailGrpc.QueryResponseMessage) = message.tuplesList.forEach {

    }
}
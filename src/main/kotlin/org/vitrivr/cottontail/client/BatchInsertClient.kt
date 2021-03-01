package org.vitrivr.cottontail.client

import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import io.grpc.Status
import io.grpc.stub.StreamObserver
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.stub.SimpleClient
import org.vitrivr.cottontail.grpc.DMLGrpc
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean

/**
 * A client for BATCH INSERTS, e.g. for importing large amounts of data.
 *
 * @author Ralph Gasser
 * @version 1.0.0
 */
class BatchInsertClient(private val channel: ManagedChannel, private val maxBuffer: Int = 1000) {

    /** Endpoint used for managing data Cottontail DB. */
    private val dml = DMLGrpc.newStub(this.channel)

    /** A [Semaphore] to prevent buffers from being flooded. */
    val semaphore = Semaphore(this.maxBuffer)

    /** A flag indicating, that [Insert] is done.. */
    val done = AtomicBoolean(false)

    /** [StreamObserver] used internally by this [BatchInsertClient]. */
    private val clientObserver = object: StreamObserver<Empty> {
        override fun onNext(value: Empty) {
            this@BatchInsertClient.semaphore.release()
        }
        override fun onCompleted() {
            this@BatchInsertClient.done.set(true)
        }
        override fun onError(t: Throwable?) {
            this@BatchInsertClient.done.set(true)
        }
    }

    /** The internal server observer object. */
    private val serverObserver = this.dml.insertBatch(this.clientObserver)

    /**
     * Constructor to create a Cottontail DB client with a new [ManagedChannel].
     *
     * @param host Host IP address of the [SimpleClient]
     * @param port The port of the [SimpleClient]
     */
    constructor(host: String, port: Int): this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build())

    /**
     * Inserts another element through this [BatchInsertClient]. This method can be called at
     * least [maxBuffer] times without blocking. After that, calling this method may block until
     * preceding messages have been processed by server.
     *
     * @param insert The [Insert] to execute.
     */
    fun insert(insert: Insert) {
        check(!this.done.get()) { "Cannot perform INSERT because client has been closed."}
        this.semaphore.acquire()
        this.serverObserver.onNext(insert.builder.build())
    }

    /**
     * Completes the INSERT through this [BatchInsertClient]. Causes all changes to be commited.
     */
    fun complete() {
        check(!this.done.get()) { "Cannot complete INSERT because client has been closed."}
        this.serverObserver.onCompleted()
        while (!this.done.get()) {
            Thread.onSpinWait()
        }
    }

    /**
     * Aborts the INSERT through this [BatchInsertClient]. Causes all changes to be rolled back.
     */
    fun abort() {
        check(!this.done.get()) { "Cannot abort INSERT because client has been closed."}
        this.serverObserver.onError(Status.ABORTED.withDescription("Transaction was aborted by client.").asException())
        while (!this.done.get()) {
            Thread.onSpinWait()
        }
    }
}
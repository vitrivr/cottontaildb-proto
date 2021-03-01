package org.vitrivr.cottontail.client.stub

import com.google.protobuf.Empty
import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import org.vitrivr.cottontail.client.TupleIterator
import org.vitrivr.cottontail.client.language.ddl.*
import org.vitrivr.cottontail.client.language.dml.Delete
import org.vitrivr.cottontail.client.language.dml.Insert
import org.vitrivr.cottontail.client.language.dql.Query
import org.vitrivr.cottontail.client.language.dml.Update
import org.vitrivr.cottontail.grpc.*

/**
 * A simple Cottontail DB client for querying and data management.
 *
 * @author Ralph Gasser
 * @version 1.1.0
 */
class SimpleClient(private val channel: ManagedChannel) {

    /** Endpoint used for executing a query through Cottontail DB. */
    private val dql by lazy { DQLGrpc.newBlockingStub(this.channel)  }

    /** Endpoint used for managing data Cottontail DB. */
    private val dml by lazy { DMLGrpc.newBlockingStub(this.channel)  }

    /** Endpoint used for managing data Cottontail DB. */
    private val ddl by lazy { DDLGrpc.newBlockingStub(this.channel)  }

    /** Endpoint used for transaction management through Cottontail DB. */
    private val tx by lazy { TXNGrpc.newBlockingStub(this.channel)  }

    /**
     * Constructor to create a Cottontail DB client with a new [ManagedChannel].
     *
     * @param host Host IP address of the [SimpleClient]
     * @param port The port of the [SimpleClient]
     */
    constructor(host: String, port: Int): this(ManagedChannelBuilder.forAddress(host, port).usePlaintext().build())

    /**
     * Begins a new transaction through this [SimpleClient].
     *
     * @return The ID of the newly begun transaction.
     */
    fun begin(): Long = this.tx.begin(Empty.getDefaultInstance()).value

    /**
     * Commits a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to commit.
     */
    fun commit(txId: Long) {
        val tx = CottontailGrpc.TransactionId.newBuilder().setValue(txId).build()
        this.tx.commit(tx)
    }

    /**
     * Rolls back a transaction through this [SimpleClient].
     *
     * @param txId The transaction ID to rollback.
     */
    fun rollback(txId: Long) {
        val tx = CottontailGrpc.TransactionId.newBuilder().setValue(txId).build()
        this.tx.rollback(tx)
    }

    /**
     * Executes [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.Query] to execute.
     */
    fun query(query: CottontailGrpc.QueryMessage): TupleIterator = TupleIterator(this.dql.query(query))

    /**
     * Executes [Query] through this [SimpleClient]
     *
     * @param q [Query] to execute.
     * @param txId Optional transaction ID to execute the query in. Can be null!
     */
    fun query(q: Query, txId: Long? = null): TupleIterator {
        val message = CottontailGrpc.QueryMessage.newBuilder().setQuery(q.builder)
        if (txId != null) {
            message.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return this.query(message.build())
    }

    /**
     * Explains [CottontailGrpc.Query] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.Query] to executed.
     */
    fun explain(query: CottontailGrpc.QueryMessage): TupleIterator = TupleIterator(this.dql.explain(query))

    /**
     * Executes [Query] through this [SimpleClient]
     *
     * @param q [Query] to execute.
     * @param txId Optional transaction ID to execute the query in. Can be null!
     */
    fun explain(q: Query, txId: Long? = null): TupleIterator {
        val message = CottontailGrpc.QueryMessage.newBuilder().setQuery(q.builder)
        if (txId != null) {
            message.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return this.explain(message.build())
    }

    /**
     * Executes this [CottontailGrpc.InsertMessage] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.InsertMessage] to execute.
     */
    fun insert(query: CottontailGrpc.InsertMessage): CottontailGrpc.QueryResponseMessage = this.dml.insert(query)

    /**
     * Executes this [Insert] through this [SimpleClient]
     *
     * @param query [Insert] to execute.
     */
    fun insert(query: Insert, txId: Long? = null): CottontailGrpc.QueryResponseMessage {
        if (txId != null) {
            query.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return this.insert(query.builder.build())
    }

    /**
     * Executes this [CottontailGrpc.UpdateMessage] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.UpdateMessage] to execute.
     */
    fun update(query: CottontailGrpc.UpdateMessage): CottontailGrpc.QueryResponseMessage = this.dml.update(query)

    /**
     * Executes this [Update] through this [SimpleClient]
     *
     * @param query [Update] to execute.
     */
    fun insert(query: Update, txId: Long? = null): CottontailGrpc.QueryResponseMessage {
        if (txId != null) {
            query.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return this.update(query.builder.build())
    }

    /**
     * Explains [CottontailGrpc.DeleteMessage] through this [SimpleClient]
     *
     * @param query [CottontailGrpc.DeleteMessage] to execute.
     */
    fun delete(query: CottontailGrpc.DeleteMessage): CottontailGrpc.QueryResponseMessage = this.dml.delete(query)

    /**
     * Executes this [Delete] through this [SimpleClient]
     *
     * @param query [Delete] to execute.
     */
    fun delete(query: Delete, txId: Long? = null): CottontailGrpc.QueryResponseMessage {
        if (txId != null) {
            query.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return this.delete(query.builder.build())
    }

    /**
     * Creates a new schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateSchemaMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun create(message: CottontailGrpc.CreateSchemaMessage): CottontailGrpc.QueryResponseMessage = this.ddl.createSchema(message)

    /**
     * Creates a new schema through this [SimpleClient].
     *
     * @param message [CreateSchema] to execute.
     * @return [TupleIterator]
     */
    fun create(message: CreateSchema, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.create(message.builder.build()))
    }

    /**
     * Creates a new entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateEntityMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun create(message: CottontailGrpc.CreateEntityMessage): CottontailGrpc.QueryResponseMessage = this.ddl.createEntity(message)

    /**
     * Creates a new entity through this [SimpleClient].
     *
     * @param message [CreateEntity] to execute.
     * @return [TupleIterator]
     */
    fun create(message: CreateEntity, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.create(message.builder.build()))
    }

    /**
     * Creates a new index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.CreateIndexMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun create(message: CottontailGrpc.CreateIndexMessage): CottontailGrpc.QueryResponseMessage = this.ddl.createIndex(message)

    /**
     * Creates a new index through this [SimpleClient].
     *
     * @param message [CreateIndex] to execute.
     * @return [TupleIterator]
     */
    fun create(message: CreateIndex, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.create(message.builder.build()))
    }

    /**
     * Drops a schema through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropSchemaMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun drop(message: CottontailGrpc.DropSchemaMessage): CottontailGrpc.QueryResponseMessage = this.ddl.dropSchema(message)

    /**
     * Drops a schema through this [SimpleClient].
     *
     * @param message [CreateIndex] to execute.
     * @return [TupleIterator]
     */
    fun drop(message: DropSchema, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.drop(message.builder.build()))
    }

    /**
     * Drops an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropEntityMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun drop(message: CottontailGrpc.DropEntityMessage): CottontailGrpc.QueryResponseMessage = this.ddl.dropEntity(message)

    /**
     * Drops an entity through this [SimpleClient].
     *
     * @param message [DropEntity] to execute.
     * @return [TupleIterator]
     */
    fun drop(message: DropEntity, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.drop(message.builder.build()))
    }

    /**
     * Drops an index through this [SimpleClient].
     *
     * @param message [CottontailGrpc.DropIndexMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun drop(message: CottontailGrpc.DropIndexMessage): CottontailGrpc.QueryResponseMessage = this.ddl.dropIndex(message)

    /**
     * Drops an index through this [SimpleClient].
     *
     * @param message [DropIndex] to execute.
     * @return [TupleIterator]
     */
    fun drop(message: DropIndex, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.drop(message.builder.build()))
    }

    /**
     * Optimizes an entity through this [SimpleClient].
     *
     * @param message [CottontailGrpc.OptimizeEntityMessage] to execute.
     * @return [CottontailGrpc.QueryResponseMessage]
     */
    fun optimize(message: CottontailGrpc.OptimizeEntityMessage): CottontailGrpc.QueryResponseMessage = this.ddl.optimizeEntity(message)

    /**
     * Optimizes an entity through this [SimpleClient].
     *
     * @param message [OptimizeEntity] to execute.
     * @return [TupleIterator]
     */
    fun optimize(message: OptimizeEntity, txId: Long? = null): TupleIterator {
        if (txId != null) {
            message.builder.setTxId(CottontailGrpc.TransactionId.newBuilder().setValue(txId))
        }
        return TupleIterator(this.optimize(message.builder.build()))
    }

    /**
     * Pings this Cottontail DB instance. Once this method returns, the ping was successful-
     */
    fun ping() {
        this.dql.ping(Empty.getDefaultInstance())
        return
    }
}
package integrations.kafka

import kotlinx.coroutines.runBlocking
import org.apache.kafka.clients.producer.ProducerRecord
import org.hamcrest.Matchers
import org.junit.Test
import org.neo4j.function.ThrowingSupplier
import streams.Assert
import streams.events.Constraint
import streams.events.Meta
import streams.events.NodeChange
import streams.events.NodePayload
import streams.events.OperationType
import streams.events.RelationshipChange
import streams.events.RelationshipNodeChange
import streams.events.RelationshipPayload
import streams.events.Schema
import streams.events.StreamsConstraintType
import streams.events.StreamsTransactionEvent
import streams.extensions.execute
import streams.serialization.JSONUtils
import streams.setConfig
import streams.start
import java.util.UUID
import java.util.concurrent.TimeUnit

class KafkaEventSinkCDCTSE: KafkaEventSinkBaseTSE() {

    @Test
    fun shouldWriteDataFromSinkWithCDCTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cdc.sourceId", topic)
        db.setConfig("streams.sink.topic.cdc.sourceId.idName", "customIdN@me")
        db.setConfig("streams.sink.topic.cdc.sourceId.labelName", "CustomLabelN@me")
        db.start()

        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = Schema()
        )
        val cdcDataEnd = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "comp@ny" to "Neo4j"), labels = listOf("User Ext"))
                ),
                schema = Schema()
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "3",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = emptyMap()),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User Ext"), ids = emptyMap()),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = Schema()
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User:`CustomLabelN@me`{name:'Andrea', `comp@ny`:'LARUS-BA', `customIdN@me`: '0'})-[r:`KNOWS WHO`{since:2014, `customIdN@me`: '3'}]->(e:`User Ext`:`CustomLabelN@me`{name:'Michael', `comp@ny`:'Neo4j', `customIdN@me`: '1'})
                |RETURN count(p) AS count""".trimMargin()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)

    }

    @Test
    fun shouldWriteDataFromSinkWithCDCSchemaTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cdc.schema", topic)
        db.start()

        val constraints = listOf(Constraint(label = "User", type = StreamsConstraintType.UNIQUE, properties = setOf("name", "surname")))
        val relSchema = Schema(properties = mapOf("since" to "Long"), constraints = constraints)
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = constraints)
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "0",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataEnd = StreamsTransactionEvent(
                        meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 1,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = NodePayload(id = "1",
                        before = null,
                        after = NodeChange(properties = mapOf("name" to "Michael", "surname" to "Hunger", "comp@ny" to "Neo4j"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )
        val cdcDataRelationship = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 2,
                        txEventsCount = 3,
                        operation = OperationType.created
                ),
                payload = RelationshipPayload(
                        id = "2",
                        start = RelationshipNodeChange(id = "0", labels = listOf("User"), ids = mapOf("name" to "Andrea", "surname" to "Santurbano")),
                        end = RelationshipNodeChange(id = "1", labels = listOf("User"), ids = mapOf("name" to "Michael", "surname" to "Hunger")),
                        after = RelationshipChange(properties = mapOf("since" to 2014)),
                        before = null,
                        label = "KNOWS WHO"
                ),
                schema = relSchema
        )
        var producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataEnd))
        kafkaProducer.send(producerRecord).get()
        producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataRelationship))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 1L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }

    @Test
    fun shouldDeleteDataFromSinkWithCDCSchemaTopic() = runBlocking {
        val topic = UUID.randomUUID().toString()
        db.setConfig("streams.sink.topic.cdc.schema", topic)
        db.start()

        db.execute("CREATE (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})")
        val nodeSchema = Schema(properties = mapOf("name" to "String", "surname" to "String", "comp@ny" to "String"),
                constraints = listOf(Constraint(label = "User", type =  StreamsConstraintType.UNIQUE, properties = setOf("name", "surname"))))
        val cdcDataStart = StreamsTransactionEvent(
                meta = Meta(timestamp = System.currentTimeMillis(),
                        username = "user",
                        txId = 1,
                        txEventId = 0,
                        txEventsCount = 3,
                        operation = OperationType.deleted
                ),
                payload = NodePayload(id = "0",
                        after = null,
                        before = NodeChange(properties = mapOf("name" to "Andrea", "surname" to "Santurbano", "comp@ny" to "LARUS-BA"), labels = listOf("User"))
                ),
                schema = nodeSchema
        )

        val producerRecord = ProducerRecord(topic, UUID.randomUUID().toString(), JSONUtils.writeValueAsBytes(cdcDataStart))
        kafkaProducer.send(producerRecord).get()

        Assert.assertEventually(ThrowingSupplier<Boolean, Exception> {
            val query = """
                |MATCH p = (s:User{name:'Andrea', surname:'Santurbano', `comp@ny`:'LARUS-BA'})-[r:`KNOWS WHO`{since:2014}]->(e:User{name:'Michael', surname:'Hunger', `comp@ny`:'Neo4j'})
                |RETURN count(p) AS count
                |""".trimMargin()
            db.execute(query) {
                val result = it.columnAs<Long>("count")
                result.hasNext() && result.next() == 0L && !result.hasNext()
            }
        }, Matchers.equalTo(true), 30, TimeUnit.SECONDS)
    }
}
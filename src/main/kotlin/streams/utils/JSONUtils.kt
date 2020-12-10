package streams.serialization

import com.fasterxml.jackson.core.JsonGenerator
import com.fasterxml.jackson.core.JsonProcessingException
import com.fasterxml.jackson.databind.*
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.IOException
import java.time.temporal.TemporalAccessor

class TemporalAccessorSerializer : JsonSerializer<TemporalAccessor>() {
    @Throws(IOException::class, JsonProcessingException::class)
    override fun serialize(value: TemporalAccessor?, jgen: JsonGenerator,
                           provider: SerializerProvider) {
        if (value == null) {
            return
        }
        jgen.writeString(value.toString())
    }
}


object JSONUtils {

    private val OBJECT_MAPPER: ObjectMapper = jacksonObjectMapper()

    init {
        val module = SimpleModule("PersistentActionSerializer")
        module.addSerializer(TemporalAccessor::class.java, TemporalAccessorSerializer())
        OBJECT_MAPPER.registerModule(module)
        OBJECT_MAPPER.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS)
        OBJECT_MAPPER.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }

    fun writeValueAsString(any: Any): String {
        return OBJECT_MAPPER.writeValueAsString(any)
    }

}
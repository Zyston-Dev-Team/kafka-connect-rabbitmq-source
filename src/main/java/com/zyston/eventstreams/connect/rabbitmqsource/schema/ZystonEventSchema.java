package com.zyston.eventstreams.connect.rabbitmqsource.schema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;

public class ZystonEventSchema {

    public static final String FIELD_MESSAGE_ID = "event_id";
    public static final String FIELD_MESSAGE_TIMESTAMP = "timestamp";
    public static final String FIELD_MESSAGE_PAYLOAD = "payload";
    public static final String FIELD_MESSAGE_COMMAND_ID = "command_id";
    public static final String FIELD_MESSAGE_COMMENT = "comment";

    static final Schema SCHEMA = SchemaBuilder.struct()
            .name("com.zyston.Event")
            .doc("Zyston Event RabbitMQ Payload")
            .field(FIELD_MESSAGE_ID, Schema.STRING_SCHEMA)
            .field(FIELD_MESSAGE_TIMESTAMP, Schema.STRING_SCHEMA)
            .field(FIELD_MESSAGE_COMMAND_ID, Schema.STRING_SCHEMA)
            .field(FIELD_MESSAGE_COMMENT, Schema.OPTIONAL_STRING_SCHEMA)
            .field(FIELD_MESSAGE_PAYLOAD, SchemaBuilder.string().build())
            .build();

    public static Struct toStruct(JsonNode zystonEvent, String payload) {
        return new Struct(SCHEMA)
                .put(FIELD_MESSAGE_ID, zystonEvent.get(FIELD_MESSAGE_ID).asText())
                .put(FIELD_MESSAGE_TIMESTAMP, zystonEvent.get(FIELD_MESSAGE_TIMESTAMP).asText())
                .put(FIELD_MESSAGE_COMMAND_ID, zystonEvent.get(FIELD_MESSAGE_COMMAND_ID).asText())
                .put(FIELD_MESSAGE_COMMENT, zystonEvent.get(FIELD_MESSAGE_COMMENT).asText())
                .put(FIELD_MESSAGE_PAYLOAD, payload);
    }
}

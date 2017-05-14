package com.spacentime.mb.common;

import java.io.Closeable;
import java.util.Map;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * Serializer class using Kyro library. 
 * @author subrsaha
 *
 */
public class KafkaEventKyroSerializer implements Closeable, AutoCloseable, Serializer<KafkaEvent>, Deserializer<KafkaEvent> {
	
    private static final int BUFFER_OUTPUT_READ_CAPACITY_100 = 100;
    
	private ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.addDefaultSerializer(KafkaEvent.class, new KryoInternalSerializerForKafkaEvent());
            return kryo;
        };
    };

    @Override
    public void configure(Map<String, ?> map, boolean b) {
    	
    }

    @Override
    public byte[] serialize(String s, KafkaEvent kafkaEvent) {
        ByteBufferOutput output = new ByteBufferOutput(BUFFER_OUTPUT_READ_CAPACITY_100);
        kryos.get().writeObject(output, kafkaEvent);
        return output.toBytes();
    }

    @Override
    public KafkaEvent deserialize(String s, byte[] bytes) {
        try {
            return kryos.get().readObject(new ByteBufferInput(bytes), KafkaEvent.class);
        }
        catch(Exception e) {
            throw new IllegalArgumentException("Error reading bytes",e);
        }
    }

    @Override
    public void close() {

    }

    private static class KryoInternalSerializerForKafkaEvent extends com.esotericsoftware.kryo.Serializer<KafkaEvent> {
        @Override
        public void write(Kryo kryo, Output output, KafkaEvent kafkaEvent) {
        	output.writeString(kafkaEvent.getUserIDForEvent());
            output.writeInt(kafkaEvent.getSource());
            output.writeString(kafkaEvent.getEventValue());
        }

        @Override
        public KafkaEvent read(Kryo kryo, Input input, Class<KafkaEvent> aClass) {
            return new KafkaEvent(input.readString(),input.readInt(),input.readString());
        }
    }

}

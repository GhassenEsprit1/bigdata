package org.example.Fuctions.Receiver;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.Fuctions.StringToVille;
import org.example.Ville.beans.Ville;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
@Slf4j
@RequiredArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<Ville>> {
     final List<String> topics;
     final JavaStreamingContext jsc;

     final Map<String,Object> kafkaParams =new HashMap<String,Object>()
    {{
        put("bootstrap.servers","localhost:9092");
        put("key.deserializer", StringDeserializer.class);
        put("value.deserializer",StringDeserializer.class);
        put("group.id","spark-kafka-integ");
        put("auto.offset.reset","earliest");
    }};

    @Override
    public JavaDStream<Ville> get() {
 JavaInputDStream<ConsumerRecord<String,String>> directStream = KafkaUtils.createDirectStream(
         jsc,
         LocationStrategies.PreferConsistent(),
         ConsumerStrategies.Subscribe(topics,kafkaParams)
 );
        StringToVille dr=new StringToVille();

 JavaDStream<Ville> javaDStream=directStream.map(ConsumerRecord::value).map(t->dr.call(t));
        return javaDStream;
    }
}

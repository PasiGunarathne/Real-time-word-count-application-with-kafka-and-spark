import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by pasindu on 4/7/19.
 */
public class SparkAPI {

    public static void main(String[] args) {
        System.out.println("hello java spark");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> wordCountInput = builder.stream("word-count-input");
        KTable<String, Long> wordCounts = wordCountInput
                .mapValues(textLine -> textLine.toLowerCase())
                .flatMapValues(lowerCasedTextLine -> Arrays.asList(lowerCasedTextLine.split(" ")))
                .selectKey((ignoredKey, word) -> word)
                .groupByKey()
                .count();

        wordCounts.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));



    }


    public void wordCount(){

    }



}

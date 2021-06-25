package com.github.binanceproducer;

import com.binance.client.SubscriptionClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class BinanceProducer {
    Logger logger = LoggerFactory.getLogger(BinanceProducer.class.getName());

    public BinanceProducer(){}
    public static void main(String[] args) {
        new BinanceProducer().run();
    }
    public void run(){
        logger.info("Setup");
        //Creating JSON object
        JSONObject object = new JSONObject();

        //Creating Kafka Producer
        KafkaProducer<String,String> producer = createKafkaProducer();

        SubscriptionClient client = SubscriptionClient.create();
        //Websocket setup for coin
        client.subscribeAggregateTradeEvent("adausdt",((event) -> {
            object.put("price", event.getPrice());
            object.put("quantity",event.getQty());
            object.put("time",event.getTime());
            producer.send(new ProducerRecord<>("aggTrade","adausdt",object.toJSONString()));
        }),null);

        client.subscribeAggregateTradeEvent("dotusdt",((aggregateTradeEvent) -> {
            object.put("price", aggregateTradeEvent.getPrice());
            object.put("quantity",aggregateTradeEvent.getQty());
            object.put("time",aggregateTradeEvent.getTime());
            producer.send(new ProducerRecord<>("aggTrade","dotusdt", object.toJSONString()));
        }),null);




    }

    private KafkaProducer<String, String> createKafkaProducer() {
        String bottstrapServer = "127.0.0.1:9092";

        //Creating producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bottstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Safe producer configs
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }
}

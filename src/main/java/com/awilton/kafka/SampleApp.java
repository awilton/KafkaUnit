package com.awilton.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.kafka.embedded.EmbeddedKafka;

public class SampleApp {
  private static final Logger log = LoggerFactory.getLogger(SampleApp.class);
  private static final String topic = "TestTopic";
  
  public static void main(String[] args) throws InterruptedException {
    EmbeddedKafka<String,String> kafka = new EmbeddedKafka<String,String>(5100,5101);
    for (int i=0; i<5; i++) {
      Thread.sleep(500);
      kafka.getProducer().send(topic, "Hello" + i);
    }
    kafka.shutdown();
    System.exit(0);
  }
  
}

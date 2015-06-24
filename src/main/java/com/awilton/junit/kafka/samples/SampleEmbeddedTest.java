package com.awilton.junit.kafka.samples;

import org.junit.Test;

import com.awilton.junit.kafka.embedded.EmbeddedKafka;

public class SampleEmbeddedTest {
  private static final EmbeddedKafka<String,String> kafka = new EmbeddedKafka<String,String>(5200,5201);
  private final String topic = "EmbeddedTopic";
  
  @Test
  public void sampleEmbeddedTest() throws Exception {
    for (int i=0;i<5;i++) {
      Thread.sleep(500);
      kafka.getProducer().send(topic, "Embedded:"+i);
    }
  }
  
}

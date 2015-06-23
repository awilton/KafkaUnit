package com.awilton.kafka;

import org.junit.Test;

import com.awilton.kafka.embedded.EmbeddedKafka;

public class SampleEmbeddedTest {
  private static final EmbeddedKafka<String,String> kafka = new EmbeddedKafka<String,String>(5200,5201);
  private final String topic = "EmbeddedTopic";
  
  @Test
  public void sampleEmbeddedTest() throws Exception {
    for (int i=0;i<100;i++) {
      Thread.sleep(5000);
      kafka.getProducer().send(topic, "Embedded:"+i);
    }
  }
  
}

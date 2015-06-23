package com.awilton.kafka;

import static org.junit.Assert.assertNotNull;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.kafka.annotations.KafkaProducer;
import com.awilton.kafka.annotations.UseKafka;
import com.awilton.kafka.runners.KafkaRunner;

@UseKafka(zkPort=5100, kafkaPort=5101)
@RunWith(KafkaRunner.class)
public class SampleAnnotatedTest {
  private static Logger log = LoggerFactory.getLogger("SampleAnnotatedTest");
  private static final String topic = "AnnotationTestTopic";
  
  @KafkaProducer
  Producer p;
  
  @Test
  public void AnnotationTestA() throws Exception {
    log.info("Testing");
    assertNotNull(p);
    KeyedMessage<String, String> km = new KeyedMessage<String, String>(topic,"foo");
    p.send(km);
    
  }
  
  
  @Test
  public void TestTwo() throws Exception {
    log.info("Test Two");
    assertNotNull(p);
  }
}

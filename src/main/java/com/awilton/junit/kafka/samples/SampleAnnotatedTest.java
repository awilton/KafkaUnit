package com.awilton.junit.kafka.samples;

import static org.junit.Assert.assertNotNull;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.junit.kafka.annotations.KafkaProducer;
import com.awilton.junit.kafka.annotations.UseKafka;
import com.awilton.junit.kafka.runners.KafkaRunner;

@UseKafka(zkPort=5100, kafkaPort=5101)
@RunWith(KafkaRunner.class)
public class SampleAnnotatedTest {
  private static Logger log = LoggerFactory.getLogger("SampleAnnotatedTest");
  private static final String topic = "AnnotationTestTopic";
  
  @KafkaProducer
  Producer<String,String> p;
  
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

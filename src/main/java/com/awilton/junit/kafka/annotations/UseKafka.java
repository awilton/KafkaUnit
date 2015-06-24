package com.awilton.junit.kafka.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * When used in conjunction with <code>KafkaRunner</code>
 * the <code>UseKafka</code> annotation instructs JUnit that the 
 * test class requires an embedded Kafka instance.
 * 
 * The <code>UseKafka</code> annotation requires two parameters.
 * The first, <code>zkPort</code>, declares the network port that the embedded
 * Zookeeper service will bind to and listen on.
 * If not specified Zookeeper will attempt to listen 
 * 
 * The second optional parameter, <code>kafkaPort</code> declares the network 
 * port that the embedded Kafka Server will bind to and listen on
 * 
 * <code>
 * 
 * {@literal @}UseKafka(zkPort=5100, kafkaPort=5101)
 * {@literal @}RunWith(KafkaRunner.class)
 * public class MyTestClass {
 *  // test methods
 * } 
 * 
 * </code>
 * 
 * @author awilton
 *
 */
@Retention(value = RetentionPolicy.RUNTIME)
public @interface UseKafka {
  int zkPort()  default 2181;
  int kafkaPort() default 9092;
}

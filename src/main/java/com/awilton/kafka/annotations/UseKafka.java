package com.awilton.kafka.annotations;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;


@Retention(value = RetentionPolicy.RUNTIME)
public @interface UseKafka {
  int zkPort()  default 2181;
  int kafkaPort() default 9092;
}

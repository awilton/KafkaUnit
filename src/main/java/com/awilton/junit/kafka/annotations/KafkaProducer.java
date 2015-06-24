package com.awilton.junit.kafka.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotated a class field that identifies a <code>javaapi.producer</code>
 * Allows for two optional parameters.
 * <code>host</host> identifies the kafka host (default is localhost)
 * <code>port</code> identifies the kafka port 
 * Default port is 9092 unless Class is annotated with UseKafka.
 * Then the port assigned in the UseKafka annotation will become the default
 * 
 * @author awilton
 *
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target({ ElementType.FIELD })
public @interface KafkaProducer {
  int port() default -1;
  String host() default "localhost";
}

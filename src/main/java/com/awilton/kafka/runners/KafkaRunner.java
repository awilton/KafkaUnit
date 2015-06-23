package com.awilton.kafka.runners;

import java.lang.reflect.Field;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.kafka.annotations.KafkaProducer;
import com.awilton.kafka.annotations.UseKafka;
import com.awilton.kafka.embedded.EmbeddedKafka;
import com.awilton.util.ReflectionUtils;

/**
 * Runner Class to be used with JUnit tests
 * 
 * <code>
 * 
 * @RunWith(KafkaRunner.class)
 * public class MyTestClass {
 *  // test methods
 * } 
 * 
 * </code>
 * 
 * @author awilton
 *
 */
public class KafkaRunner extends BlockJUnit4ClassRunner {
  private static final Logger log = LoggerFactory.getLogger(KafkaRunner.class);
  private static final int DEFAULT_KAFKA_PORT = 9092;
  private UseKafka ka = null;
  
  public KafkaRunner(Class<?> klass) throws InitializationError {
    super(klass);
    
    log.info("Starting custom runner from class:" + klass.getCanonicalName());
    this.ka = klass.getAnnotation(UseKafka.class);
    if (null != ka) {
      @SuppressWarnings("rawtypes")      
      EmbeddedKafka kafka = new EmbeddedKafka(ka.zkPort(),ka.kafkaPort());
    }
    
  }
  
  @Override
  protected void runChild(final FrameworkMethod method, RunNotifier notifier) {
    super.runChild(method, notifier);
  }
  
  @Override
  protected Object createTest() throws Exception {
    log.info("Setting up new test object");
    Object testObject = getTestClass().getOnlyConstructor().newInstance();
    
    Class<?> c = getTestClass().getJavaClass();
    log.info("TestClass is:"+c.getName());
   
    
    Field[] fields = c.getDeclaredFields();
    for (Field f:fields) {
      log.info("Testing field " + f.getName() +  " for annotations");
      if (f.isAnnotationPresent(KafkaProducer.class)) {
        log.info("Annotation for producer found:"+f.getName());
        try {
          KafkaProducer kp = f.getAnnotation(KafkaProducer.class);
          int port = kp.port();
          if (port == -1) {
            if (null != this.ka)  port = ka.kafkaPort();
          }
          ProducerConfig pc = new ProducerConfig(producerProps(kp.host(),port));
          @SuppressWarnings("rawtypes")
          Producer prod = new Producer(pc);
  
          ReflectionUtils.makeAccessible(f);
          f.set(testObject, prod);
        } catch(Throwable t) {
          throw new RuntimeException(t);
        }
      }
    }
    
    log.info("Created:"+testObject.getClass().getSimpleName());
    return testObject;
  }
  
  
  private Properties producerProps(String host, int port) {
    if (port == -1) port = DEFAULT_KAFKA_PORT;
    String kuri = host+":"+port;
    log.info("Setting producer properties:"+host+"  " + port + "(" + kuri + ")");
    
    Properties p = new Properties();
    String kHost = kuri;
    p.put("metadata.broker.list",kHost);
    p.put("serializer.class", "kafka.serializer.StringEncoder");
    p.put("request.required.acks", "1");
    
    return p;
  }
  
}

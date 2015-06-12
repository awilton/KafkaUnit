package com.awilton.kafka.runners;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.kafka.annotations.UseKafka;
import com.awilton.kafka.embedded.EmbeddedKafka;


public class KafkaRunner extends BlockJUnit4ClassRunner {
  private static final Logger log = LoggerFactory.getLogger(KafkaRunner.class);
  public  EmbeddedKafka kafka;
  
  public KafkaRunner(Class<?> klass) throws InitializationError {
    super(klass);
    
    log.info("Starting custom runner from class:" + klass.getCanonicalName());
    UseKafka ka = klass.getAnnotation(UseKafka.class);
    if (null != ka) kafka = new EmbeddedKafka(ka.zkPort(),ka.kafkaPort());
    
  }
  
//  @Override
//  public Object createTest() throws Exception {
//      log.info("---------------  C R E A T E __ T E S T ------------");
//       Object test = super.createTest();
//       return test;
//       
//  }
  
}

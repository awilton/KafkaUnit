package com.awilton.kafka.annotations;

import java.lang.reflect.Field;
import java.util.List;

import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkField;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.kafka.embedded.EmbeddedKafka;


public class KafkaRunner extends BlockJUnit4ClassRunner {
  private static final Logger log = LoggerFactory.getLogger(KafkaRunner.class);
  private EmbeddedKafka kafka;
  
  public KafkaRunner(Class<?> klass) throws InitializationError {
    super(klass);
    
    log.info("Starting custom runner from class:" + klass.getCanonicalName());
    UseKafka ka = klass.getAnnotation(UseKafka.class);
    if (null != ka) {
      log.info("UseKafka Found");
        this.kafka = new EmbeddedKafka(ka.zkPort(),ka.kafkaPort());
    }
   
  }
  
  @Override
  public Object createTest() throws Exception {
    log.info("---------------  C R E A T E __ T E S T ------------");
       Object test = super.createTest();
       prepareTest(test);
       return test;
  }
  
  private void prepareTest(Object test) throws InitializationError {
    log.info("Preparing Test");
    List<FrameworkField> ffl = this.getTestClass().getAnnotatedFields();
    if (null != ffl) {
      for (FrameworkField ff:ffl) {
        log.info("Found annotated field:"+ff.getName() + " :: " + ff.getType());
        if (ff.getType().isAssignableFrom(EmbeddedKafka.class)) {
          log.info("Found kafka producer annotation");
         setAnnotatedProducer(test,ff);
        }
      }  
    }
  }
  
  private void setAnnotatedProducer(Object test, FrameworkField fFld) throws InitializationError {
    log.info("Setting producer field:"+fFld.getName());
    try {
      fFld.getField().setAccessible(true);
      fFld.getField().set(test, this.kafka);
    } catch (Exception e) {
      log.error("Exception",e);
      throw new InitializationError(e);
    }
  }
  
  
  
  
  
  private void setProducer() throws IllegalArgumentException, IllegalAccessException {
    if (null == this.kafka) {
      log.warn("No KafakSvc annotated field found");
      return;
    }
    List<FrameworkField> ffl = this.getTestClass().getAnnotatedFields(KafkaSvc.class);
    if (null == ffl || ffl.isEmpty()) return;
    
    for (FrameworkField ff:ffl) {
      Field f = ff.getField();
      f.setAccessible(true);
      if (f.getClass().isInstance(this.kafka)) {
        log.info("Injected embedded kafka to field:"+f.getName());
        f.set(null, this.kafka);
      }
    }
  }
  
  
}

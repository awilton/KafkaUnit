package com.awilton.kafka;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.kafka.annotations.KafkaRunner;
import com.awilton.kafka.annotations.KafkaSvc;
import com.awilton.kafka.annotations.UseKafka;
import com.awilton.kafka.embedded.EmbeddedKafka;


@UseKafka(zkPort=5100, kafkaPort=5101)
@RunWith(KafkaRunner.class)
public class SampleAnnotationTest 
{
    private static Logger log = LoggerFactory.getLogger("foo");
  
    @KafkaSvc
    protected EmbeddedKafka kafka;
 
    
    @Test
    public void TestZK() throws Exception {
      log.info("Testing");
      kafka.createTopic("fooTest");
      
      for (int i=0;i<100; i++) {
        
        Thread.sleep(5000);
        kafka.sendMessage("fooTest", "TestLoop:"+i);
      }
    }
    
//    @Test
//    public void TestTwo() throws Exception {
//      log.info("Test two");
//      kafka.createTopic("fooTest");
//      kafka.sendMessage("fooTest", "GoodBye");
//    }
//    
//    @Test
//    public void TestThree() throws Exception {
//      for (int i = 0; i<500; i++) {
//        kafka.sendMessage("fooTest", "Hello::"+i);
//        Thread.sleep(5000);
//      }
//      Thread.sleep(500000);
//    }
}

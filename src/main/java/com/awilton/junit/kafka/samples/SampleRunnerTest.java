package com.awilton.junit.kafka.samples;


import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.awilton.junit.kafka.annotations.UseKafka;
import com.awilton.junit.kafka.embedded.EmbeddedProducer;
import com.awilton.junit.kafka.runners.KafkaRunner;


@UseKafka(zkPort=5100, kafkaPort=5101)
@RunWith(KafkaRunner.class)
public class SampleRunnerTest 
{
    private static Logger log = LoggerFactory.getLogger("foo");
    
    @Test
    public void TestZK() throws Exception {
      log.info("Testing");
      EmbeddedProducer<String,String> producer = new EmbeddedProducer<String,String>(5101);
      for (int i=0;i<100; i++) {
        producer.send("fooTest", "TestLoop:"+i);
        Thread.sleep(50);
      }
    }
    
}

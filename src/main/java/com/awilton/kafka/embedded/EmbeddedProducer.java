package com.awilton.kafka.embedded;

import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedProducer<X,Y> {
  private static Logger log = LoggerFactory.getLogger(EmbeddedProducer.class);
  
  private Producer<X,Y> producer;
  
  public EmbeddedProducer(int kafkaPort) {
    init(initProps(kafkaPort));
  }
  
  public EmbeddedProducer(Properties kafkaProps) {
    log.info("Initializing");
    init(kafkaProps);
  }
  
  public Producer<X,Y> getProducer() {
    return this.producer;
  }
  
  public void send(String topic, Y msg) {
    KeyedMessage<X,Y> kMsg = new KeyedMessage<X,Y>(topic,null,msg);
    this.sendMessage(kMsg);
  }
  
  public void send(List<KeyedMessage<X, Y>> messages) {
    this.producer.send(messages);
  }
  
  public void sendMessage(KeyedMessage<X, Y> msg) {
    try {
      this.producer.send(msg);
      log.debug(">>>>>>>>>>>>  Message sent");
    } catch(Exception e) {
      log.error("Failed to send",e);
      throw new RuntimeException(e);
    }
  }

  public void close() {
    if (null != producer) {
      producer.close();
    }
  }
  private void init(Properties p) {
    ProducerConfig pc = new ProducerConfig(p);
    this.producer = new Producer<X,Y>(pc);
    
  }
  
  private Properties initProps(int kfkaPort) {
    Properties p = new Properties();
    String kHost = "localhost:"+kfkaPort;
    p.put("metadata.broker.list",kHost);
    p.put("serializer.class", "kafka.serializer.StringEncoder");
    p.put("request.required.acks", "1");
    
    return p;
  }
}

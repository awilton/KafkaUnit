package com.awilton.kafka.embedded;

import java.io.File;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.collection.Seq;
import kafka.admin.AdminUtils;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

public class EmbeddedKafka {
  private static Logger log = LoggerFactory.getLogger(EmbeddedKafka.class);
  public KafkaServerStartable kafka;
  public  EmbeddedZooKeeper zookeeper;
  private ZkClient zkClient;
  private String zkHost;
  private EmbeddedProducer<String,String> producer;
  
  public EmbeddedKafka(int zkport, int kfkaport) {
    this.zookeeper = new EmbeddedZooKeeper(zkport);
    this.zkHost = "localhost:"+zkport;
    
    Properties kafkaProps = init(kfkaport);
    kafkaProps.put("zookeeper.connect", "localhost:"+zkport);
    
    log.info("Initializing kafka server with properties:" + kafkaProps.toString());
    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    kafka = new KafkaServerStartable(kafkaConfig);
    log.info("Starting Kafka");
    kafka.startup();
  
    initZkClient();
    this.producer = new EmbeddedProducer<String,String>(kfkaport);
    log.info("\n*************************************************************");
    log.info("Started");
    log.info("\n*************************************************************");
    
  }
  
  public void shutdown() {
    // shutdown the producer
    if (null != this.producer) {
      this.producer.close();
    }
    //shutdown the kafka server
    this.kafka.shutdown();
    // shutdown zookeeper
    this.zkClient.close();
  }
  public void createTopic(String topic) {
    log.info("ADMIN::Creating topic:"+topic);
    initZkClient();
    AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties());
  }
  
  public void deleteTopic(String topic) {
    log.info("ADMIN::Deleting topic:"+topic);
    initZkClient();
    AdminUtils.deleteTopic(zkClient, topic);
  }
  
  public void sendMessage(String topic, String message) {
    log.info("OP::Sending message to topic:"+topic);
    KeyedMessage<String,String> msg = new KeyedMessage<String,String>(topic,null,message);
    this.producer.send(msg);
  }
  
  public Producer<String,String> getProducer() {
    if (null == this.producer) return null;
    return this.producer.getProducer();
  }
  
  private void initZkClient() {
    if (null != this.zkClient) return;
    this. zkClient = new ZkClient(this.zkHost, 5000, 3000);
  }
  
  private Properties init(int port) {
    File tmp = new File("./ktmp");
    if (!tmp.exists()) tmp.mkdir();
    
    String stamp = Long.toString(System.currentTimeMillis());
    File log = new File("./ktmp/"+stamp);
    String logDir;
    
    int id = UUID.randomUUID().hashCode();
    if (id < 0) id = id*-1;
    
    if (!log.exists()) log.mkdir();
    if (!log.isDirectory()) {
      throw new RuntimeException("Kafka Log directory is not a directory");
    }
    try {
      logDir = log.getCanonicalPath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to process path:",e);
    }
    
    Properties p = new Properties();
    p.setProperty("hostname", "localhost");
    p.setProperty("host.name", "localhost");
    p.setProperty("num.partitions", "1");
    p.setProperty("broker.id", Integer.toString(id));
    p.setProperty("port", Integer.toString(port));
    p.setProperty("brokerid", "1");
    p.setProperty("log.dir", logDir);
    p.setProperty("enable.zookeeper", "true");
    return p;
  }
  
}

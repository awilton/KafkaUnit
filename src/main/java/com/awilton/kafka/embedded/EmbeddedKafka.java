package com.awilton.kafka.embedded;

import java.io.File;
import java.util.Properties;
import java.util.UUID;

import kafka.admin.AdminUtils;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServerStartable;

import org.I0Itec.zkclient.ZkClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedKafka<X,Y> {
  private static Logger log = LoggerFactory.getLogger(EmbeddedKafka.class);
  
  private EmbeddedZooKeeper zookeeper;
  private EmbeddedProducer<X,Y> producer = null;
  private KafkaServerStartable kafka;
  private ZkClient zkClient;
  private String zkHost;
  
  private final int kafkaPort;
  
  public EmbeddedKafka(int zkport, int kfkaport) {
    this.kafkaPort = kfkaport;
    
    zookeeper = new EmbeddedZooKeeper(zkport);
    this.zkHost = "localhost:"+zkport;
    
    Properties kafkaProps = init(kfkaport);
    kafkaProps.put("zookeeper.connect", "localhost:"+zkport);
    
    log.info("Initializing kafka server with properties:" + kafkaProps.toString());
    KafkaConfig kafkaConfig = new KafkaConfig(kafkaProps);
    
    //Startup a single kafka instance
    kafka = new KafkaServerStartable(kafkaConfig);
    log.info("Starting Kafka");
    kafka.startup();
  
    log.info("\n*************************************************************");
    log.info("Embedded Kafka Service Started");
    log.info("\n*************************************************************");
    
  }
  
  public void shutdown() {
    // shutdown the producer
    if (null != this.producer) {
      this.producer.close();
    }
    //shutdown the kafka server
    this.kafka.shutdown();
    
    // shutdown zookeeper client
    if (null != this.zkClient) this.zkClient.close();
    
    // shutdown zookeeper
    if (null != this.zookeeper) this.zookeeper.shutdown();
    
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
  
  
  public EmbeddedProducer<X,Y> getProducer() {
    if(null == this.producer) this.producer = new EmbeddedProducer<X,Y>(this.kafkaPort);
    return this.producer;
  }
  
  private void initZkClient() {
    if (null != this.zkClient) return;
    this. zkClient = new ZkClient(this.zkHost, 5000);
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

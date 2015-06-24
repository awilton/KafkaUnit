package com.awilton.junit.kafka.embedded;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmbeddedZooKeeper {
  private static Logger log = LoggerFactory.getLogger(EmbeddedZooKeeper.class);
  
  private final ZooKeeperServerMain zooKeeperServer;
  private Thread zkThread;
  
  public EmbeddedZooKeeper(int port) {
    zooKeeperServer = new ZooKeeperServerMain();
    QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
    try {
      Properties p = init(port);
      log.info("Configuring Zookeeper with properties:" + p.toString());
      quorumConfiguration.parseProperties(p);
    } catch(Exception e) {
      throw new RuntimeException(e);
    }
    
    final ServerConfig configuration = new ServerConfig();
    configuration.readFrom(quorumConfiguration);
    this.zkThread = initZkThread(configuration);
    this.zkThread.start();
  }
  
  private Thread initZkThread(final ServerConfig cfg) {
    return new Thread("EmbeddedZookeeper") {
      public void run() {
        try {
          zooKeeperServer.runFromConfig(cfg);
        } catch (IOException e) {
          log.error("Failed to initialize embedded zookeeper",e);
          throw new RuntimeException(e);
        }
      }
    };
  }
  
  public void shutdown() {
//    if (null != this.zkThread) zkThread.interrupt();
  }
  
 
  private Properties init(int port) {
    String stamp = Long.toString(System.currentTimeMillis());
    File tmp = new File("zktmp");
    if (!tmp.exists()) tmp.mkdir();
    if (!tmp.isDirectory()) {
      throw new RuntimeException("zktmp directory is a physical file");
    }
    
    File data = new File("./zktmp/zkdata."+stamp);
    File log = new File("./zktmp/zklog."+stamp);
    String dataDir, logDir;
    
    if (!data.exists()) data.mkdir();
    if (!log.exists()) log.mkdir();
    if (!data.isDirectory() || !log.isDirectory()) {
      throw new RuntimeException("Data and or Log directory is a physical file");
    }
    try {
      dataDir = data.getCanonicalPath();
      logDir = log.getCanonicalPath();
    } catch (Exception e) {
      throw new RuntimeException("Failed to process path:",e);
    }
    
    Properties p = new Properties();
    p.setProperty("clientPort", Integer.toString(port));
    p.setProperty("dataDir", dataDir);
    p.setProperty("dataLogDir", logDir);
    p.setProperty("maxClientCnxns", "0");
    p.setProperty("electionAlg","2");
    p.setProperty("tickTime", "10000"); 
    return p;
  }
  
}

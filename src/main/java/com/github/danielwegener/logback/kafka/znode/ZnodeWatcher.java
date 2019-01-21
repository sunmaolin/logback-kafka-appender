package com.github.danielwegener.logback.kafka.znode;

import com.alibaba.fastjson.JSONObject;
import com.github.danielwegener.logback.kafka.KafkaAppender;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

public class ZnodeWatcher implements Watcher {

    public static final String KAFKA_NODE = "/brokers/ids";
    public static final String ZOOKEEPER_SERVERS = "zookeeper.servers";

    private KafkaAppender kafkaAppender;
    private ZooKeeper zooKeeper;

    public ZnodeWatcher(KafkaAppender kafkaAppender, ZooKeeper zooKeeper) {
        this.kafkaAppender = kafkaAppender;
        this.zooKeeper = zooKeeper;
    }

    /**
     * 拼接所有的kafka服务器节点
     */
    public static String getKafkaNode(ZooKeeper zkClient, List<String> childs) throws KeeperException, InterruptedException {
        StringBuilder kafkaNodes = new StringBuilder();
        for (String child : childs) {
            String childStr = new String(zkClient.getData(KAFKA_NODE + "/" + child, null, null));
            JSONObject jsonObject = JSONObject.parseObject(childStr);
            String host = jsonObject.getString("host");
            Integer port = jsonObject.getInteger("port");
            kafkaNodes.append(host);
            kafkaNodes.append(":");
            kafkaNodes.append(port);
            kafkaNodes.append(",");
        }
        if (kafkaNodes.length() > 0) {
            kafkaNodes.deleteCharAt(kafkaNodes.length() - 1);
        }
        return kafkaNodes.toString();
    }

    /**
     * 获取一个可用的zookeeper节点
     */
    public static ZooKeeper getZooKeeper(List<String> servers) {
        ZooKeeper zkClient;
        for (String server : servers) {
            try {
                zkClient = new ZooKeeper(server, 120000, null);
            } catch (Exception e) {
                e.printStackTrace();
                continue;
            }
            return zkClient;
        }
        return null;
    }

    @Override
    public void process(WatchedEvent event) {
        //Event.KeeperState keeperState = event.getState();
        Event.EventType eventType = event.getType();
        String eventPath = event.getPath();
        System.out.println("监听到节点变化:eventPath" + eventPath + ",eventType:" + eventType);
        switch (eventType) {
            case None:
                break;
            case NodeCreated:
                break;
            case NodeDeleted:
                break;
            case NodeDataChanged:
                break;
            case NodeChildrenChanged:
                try {
                    if (null == zooKeeper) {
                        System.out.println("获取不到zookeeper节点");
                        return;
                    }
                    //继续监听节点
                    List<String> childs = zooKeeper.getChildren(eventPath, this);
                    String kafkaNodes = getKafkaNode(zooKeeper, childs);
                    kafkaAppender.producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, kafkaNodes);
                    System.out.println("添加kafka节点:" + kafkaNodes);
                    kafkaAppender.reset();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
            default:
                break;
        }
    }

}

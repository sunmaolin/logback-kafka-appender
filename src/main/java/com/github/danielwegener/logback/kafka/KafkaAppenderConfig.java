package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.AppenderAttachable;
import com.github.danielwegener.logback.kafka.delivery.AsynchronousDeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;
import com.github.danielwegener.logback.kafka.keying.NoKeyKeyingStrategy;
import com.github.danielwegener.logback.kafka.znode.ZnodeWatcher;
import org.apache.zookeeper.ZooKeeper;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.danielwegener.logback.kafka.znode.ZnodeWatcher.ZOOKEEPER_SERVERS;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

/**
 * @since 0.0.1
 */
public abstract class KafkaAppenderConfig<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E> {

    public Map<String, Object> producerConfig = new HashMap<>();
    protected String topic = null;
    protected Encoder<E> encoder = null;
    protected KeyingStrategy<? super E> keyingStrategy = null;
    protected DeliveryStrategy deliveryStrategy;
    protected Integer partition = null;
    protected boolean appendTimestamp = true;

    protected boolean checkPrerequisites() {
        boolean errorFree = true;

        if (producerConfig.get(ZOOKEEPER_SERVERS) == null) {
            addError("No \"zookeeper.servers\" set for the appender named [\"" + name + "\"].");
            errorFree = false;
        }
        List<String> servers = Arrays.asList(producerConfig.get(ZOOKEEPER_SERVERS).toString().split(","));
        ZooKeeper zkClient = ZnodeWatcher.getZooKeeper(servers);
        if (null == zkClient) {
            addError("\"zookeeper.servers\" is error for the appender named [\"" + name + "\"].");
            errorFree = false;
        }
        try {
            if (zkClient != null) {
                //监听节点变化
                ZnodeWatcher znodeWatcher = new ZnodeWatcher((KafkaAppender) this, zkClient);
                List<String> childs = zkClient.getChildren(ZnodeWatcher.KAFKA_NODE, null);
                String kafkaNodes = ZnodeWatcher.getKafkaNode(zkClient, childs);
                producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, kafkaNodes);
                //producerConfig.remove(ZOOKEEPER_SERVERS);
                System.out.println("添加kafka节点:" + kafkaNodes);
                Thread watcherThread = new Thread(() -> {
                    try {
                        zkClient.getChildren(ZnodeWatcher.KAFKA_NODE, znodeWatcher);
                        synchronized (this) {
                            wait();
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                });
                //希望你永远沉睡保护着监听程序
                watcherThread.setDaemon(true);
                watcherThread.start();
            }
        } catch (Exception e) {
            addError("\"zookeeper.servers\" is error for the appender named [\"" + name + "\"].");
            errorFree = false;
        }
        if (topic == null) {
            addError("No topic set for the appender named [\"" + name + "\"].");
            errorFree = false;
        }

        if (encoder == null) {
            addError("No encoder set for the appender named [\"" + name + "\"].");
            errorFree = false;
        }

        if (keyingStrategy == null) {
            addInfo("No explicit keyingStrategy set for the appender named [\"" + name + "\"]. Using default NoKeyKeyingStrategy.");
            keyingStrategy = new NoKeyKeyingStrategy();
        }

        if (deliveryStrategy == null) {
            addInfo("No explicit deliveryStrategy set for the appender named [\"" + name + "\"]. Using default asynchronous strategy.");
            deliveryStrategy = new AsynchronousDeliveryStrategy();
        }

        return errorFree;
    }

    public void setEncoder(Encoder<E> encoder) {
        this.encoder = encoder;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setKeyingStrategy(KeyingStrategy<? super E> keyingStrategy) {
        this.keyingStrategy = keyingStrategy;
    }

    public void addProducerConfig(String keyValue) {
        String[] split = keyValue.split("=", 2);
        if (split.length == 2) addProducerConfigValue(split[0], split[1]);
    }

    public void addProducerConfigValue(String key, Object value) {
        this.producerConfig.put(key, value);
    }

    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }

    public void setDeliveryStrategy(DeliveryStrategy deliveryStrategy) {
        this.deliveryStrategy = deliveryStrategy;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public boolean isAppendTimestamp() {
        return appendTimestamp;
    }

    public void setAppendTimestamp(boolean appendTimestamp) {
        this.appendTimestamp = appendTimestamp;
    }

}

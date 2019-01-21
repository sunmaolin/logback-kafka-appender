package com.github.danielwegener.logback.kafka;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;

public class ZookeeperTest implements Runnable {

    private static String servers = "192.168.25.80:2181";
    private static String znodeOne = "/znodeOne";
    private static String znodeTwo = "/znodeTwo";
    private static String znodeChild = "/znodeChild";

    public static void main(String[] args) {
        new Thread(new ZookeeperTest()).start();
    }

    public void run() {
        /*
         * 验证过程如下：
         * 1、验证一个节点X上使用exist方式注册的多个监听器（NodeWatcher、NodeWatcher），
         *      在节点X发生create事件时的事件通知情况
         * 2、验证一个节点Y上使用getDate方式注册的多个监听器（NodeWatcher、NodeWatcher），
         *      在节点X发生create事件时的事件通知情况
         * */
        //默认监听：注册默认监听是为了让None事件都由默认监听处理，
        //不干扰NodeWatcher、NodeWatcher的日志输出
        ZooKeeper zkClient;
        try {
            zkClient = new ZooKeeper(servers, 120000, new WatcherDefault());
        } catch (IOException e) {
            e.printStackTrace();
            return;
        }
        //默认监听也可以使用register方法注册
        //zkClient.register(watcherDefault);

        //1、========================================================
        //注册监听，注意，这里两次exists方法的执行返回都是null，因为“znodeOne”节点还不存在
        try {
            Stat statOne = zkClient.exists(znodeOne, new NodeWatcher(zkClient, znodeOne));
            Stat statTwo = zkClient.exists(znodeOne, new NodeWatcher(zkClient, znodeOne));
            System.out.println("statOne:" + statOne);
            System.out.println("statTwo:" + statTwo);
            zkClient.create(znodeOne, znodeOne.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }
        //TODO 注意观察日志，根据原理我们猜测理想情况下NodeWatcher和NodeWatcher都会被通知。

        //2、注册监听，注意，这里使用两次getData方法注册监听，"Y"节点目前并不存在
        try {
            byte[] oneData = zkClient.getData(znodeTwo, new NodeWatcher(zkClient, znodeTwo), null);
            byte[] twoData = zkClient.getData(znodeTwo, new NodeWatcher(zkClient, znodeTwo), null);
            System.out.println("oneData:" + new String(oneData));
            System.out.println("twoData:" + new String(twoData));
        } catch (Exception e) {
            e.printStackTrace();
        }
        //TODO 注意观察日志，因为"Y"节点不存在，所以getData就会出现异常。watcherOneY、watcherTwoY的注册都不起任何作用。
        //然后我们在报了异常的情况下，创建"Y"节点，根据原理，不会有任何watcher响应"Y"节点的create事件
        try {
            zkClient.create(znodeTwo, znodeTwo.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (Exception e) {
            e.printStackTrace();
            return;
        }

        try {
            List<String> childs = zkClient.getChildren(znodeChild, new NodeWatcher(zkClient, znodeChild));
            for (String child : childs) {
                byte[] childData = zkClient.getData(znodeChild + "/" + child, null, null);
                System.out.println(new String(childData));
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            Stat stat = zkClient.exists(znodeChild, null);
            if (null == stat) {
                zkClient.create(znodeChild, znodeChild.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            for (int i = 0; i < 10; i++) {
                zkClient.create(znodeChild + "/" + i, znodeChild.getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                Thread.sleep(1000);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("监听完成");
        //下面这段代码可以忽略，是为了观察zk的原理。让守护线程保持不退出
        synchronized (this) {
            try {
                this.wait();
            } catch (InterruptedException e) {
                e.printStackTrace();
                System.exit(-1);
            }
        }
    }
}

class WatcherDefault implements Watcher {

    public void process(WatchedEvent event) {
        KeeperState keeperState = event.getState();
        EventType eventType = event.getType();
        String eventPath = event.getPath();
        System.out.println("ManyWatcherDefaultkeeperState:" + keeperState + ",eventType:" + eventType + ",eventPath:" + eventPath);
    }
}

class NodeWatcher implements Watcher {

    private ZooKeeper zkClient;
    private String watcherPath;

    public NodeWatcher(ZooKeeper zkClient, String watcherPath) {
        this.zkClient = zkClient;
        this.watcherPath = watcherPath;
    }

    public void process(WatchedEvent event) {
        try {
            Stat stat = this.zkClient.exists(this.watcherPath, this);
        } catch (Exception e) {
            e.printStackTrace();
        }
        KeeperState keeperState = event.getState();
        EventType eventType = event.getType();
        //这个属性是发生事件的path
        String eventPath = event.getPath();
        System.out.println("keeperState:" + keeperState + ",eventType:" + eventType + ",eventPath:" + eventPath);

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
                    //继续监听
                    zkClient.getChildren(eventPath, this);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                break;
        }
    }
}

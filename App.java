package com.it18zhang.stormdemo.wc;

import com.it18zhang.stormdemo.CallLogCounterBolt;
import com.it18zhang.stormdemo.CallLogCreatorBolt;
import com.it18zhang.stormdemo.CallLogSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

/**
 * App
 */
public class App {
    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        //设置Spout
        builder.setSpout("wcspout", new WordCountSpout());
        //设置creator-Bolt
        builder.setBolt("split-bolt", new SplitBolt()).shuffleGrouping("wcspout");
        //设置counter-Bolt
        builder.setBolt("counter-bolt", new CountBolt()).fieldsGrouping("split-bolt", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        /**
         * 本地模式storm
         */
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("wc", conf, builder.createTopology());
        Thread.sleep(10000);
        //StormSubmitter.submitTopology("mytop", conf, builder.createTopology());
        cluster.shutdown();

    }
}

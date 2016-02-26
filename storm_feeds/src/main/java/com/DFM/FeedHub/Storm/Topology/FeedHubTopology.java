package com.DFM.FeedHub.Storm.Topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.DFM.FeedHub.Storm.Bolt.*;
import com.DFM.FeedHub.Storm.Spout.PublisherSpout;

import java.util.HashMap;
import java.util.Map;

public class FeedHubTopology {
    private static StormTopology buildTopology(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        // Spout to get feeds names from Redis
        builder.setSpout("PublisherSpout", new PublisherSpout(), 4).setNumTasks(1);
        // Storm to get feed URL from Redis based on feed name
        builder.setBolt("PublisherBolt", new PublisherBolt(), 4).setNumTasks(2).shuffleGrouping("PublisherSpout");
        // Storm to determine feed type and send to appropriate bolt based on feed class
        builder.setBolt("FeedLoadBolt", new FeedLoadBolt(), 4).setNumTasks(2).shuffleGrouping("PublisherBolt");
        // Storm to process Feed class
        builder.setBolt("FeedReaderBolt", new FeedReaderBolt(), 4).setNumTasks(2).shuffleGrouping("FeedLoadBolt");
        // Storm to extract Feed Item data
        builder.setBolt("SinglePhaseLoadBolt", new SinglePhaseLoadBolt(), 4).setNumTasks(2).shuffleGrouping("FeedReaderBolt", "SinglePhase");
        // Storm to extract Feed Item data
        builder.setBolt("MultiPhaseLoadBolt", new MultiPhaseLoadBolt(), 4).setNumTasks(2).shuffleGrouping("FeedReaderBolt", "MultiPhase");
        // Storm to extract Feed Item data
        builder.setBolt("StoryReaderBolt", new StoryReaderBolt(), 4).setNumTasks(5).fieldsGrouping("SinglePhaseLoadBolt", new Fields("storyGUID")).fieldsGrouping("MultiPhaseLoadBolt", new Fields("storyGUID"));
        // Storm to extract Item data
        builder.setBolt("SubscriberBolt", new SubscriberBolt(), 4).setNumTasks(5).fieldsGrouping("StoryReaderBolt", new Fields("storyGUID"));
        // Storm to extract Publish Item data
        builder.setBolt("WordPressBolt", new WordPressBolt(), 4).setNumTasks(10).fieldsGrouping("SubscriberBolt", "WordPress", new Fields("storyGUID"));
        //builder.setBolt("OtherBolt", new WordPressBolt(), 4).setNumTasks(4).shuffleGrouping("SubscriberBolt", "Other");
        return builder.createTopology();
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        int i = 0;
        String key = "";
        String val = "";
        Map<String, String> options = new HashMap<String, String>();
        while (i < args.length) {
            if (args[i].startsWith("-")) {
                key = args[i].replace("-", "");
            } else {
                val = args[i];
            }
            options.put(key, val);
            i++;
        }

        if (options.get("mode").equalsIgnoreCase("storm")) {
            if (options.get("loc").equalsIgnoreCase("live")) {
                stormLive(args);
            }
            if (options.get("loc").equalsIgnoreCase("local")) {
                stormLocal(args);
            }
        }

        if (options.get("mode").equalsIgnoreCase("linear")) {
            if (options.get("loc").equalsIgnoreCase("live")) {
                linearLive(args);
            }
            if (options.get("loc").equalsIgnoreCase("local")) {
                linearLocal(args);
            }
        }
    }

    private static void stormLive(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(4);
        conf.setMaxSpoutPending(100);
        conf.setMessageTimeoutSecs(300);
        //conf.setMaxTaskParallelism(4);
        conf.put("redisHost", "redis address");
        conf.put("redisPort", "6379");
        conf.put("redisTimeout", "30000");
        conf.put("redisPassword", "redis password");
        conf.put("publisherKey", "publishers:*");
        conf.put("xsltRootPath", "/etc/storm/transactstorm/conf/xslt/");

        // PubSubTopology is the name of submitted topology
        //Submit to prod cluster uncomment next command, comment previous 2 commands
        try {
            StormSubmitter.submitTopology("FeedHubTopology", conf, buildTopology(args));
        } catch (Exception ignored) {
        }
    }

    private static void stormLocal(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setNumWorkers(4);
        conf.setMaxSpoutPending(100);
        conf.setMessageTimeoutSecs(300);
        //conf.setMaxTaskParallelism(4);
        conf.put("redisHost", "redis address");
        conf.put("redisPort", "6379");
        conf.put("redisTimeout", "30000");
        conf.put("redisPassword", "redis password");
        conf.put("publisherKey", "publishers:*");
        conf.put("xsltRootPath", "/etc/storm/transactstorm/conf/xslt/");

        // PubSubTopology is the name of submitted topology.
        // create an instance of LocalCluster class for executing topology in local mode.
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("FeedHubTopology", conf, buildTopology(args));
    }

    private static void linearLocal(String[] args) {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("redisHost", "172.28.128.3");
        conf.put("redisPort", "6379");
        conf.put("redisTimeout", "30000");
        conf.put("redisPassword", "");
        conf.put("publisherKey", "publishers:*");
        conf.put("xsltRootPath", "C:\\Users\\Mick\\Documents\\Projects\\FeedHub\\storm_feeds\\src\\xslt\\");

        PublisherSpout publisherSpout = new PublisherSpout();
        publisherSpout.linear(conf);
    }

    private static void linearLive(String[] args) {
        Map<String, String> conf = new HashMap<String, String>();
        conf.put("redisHost", "ec2-52-35-19-0.us-west-2.compute.amazonaws.com");
        conf.put("redisPort", "6379");
        conf.put("redisTimeout", "30000");
        conf.put("redisPassword", "bkT$esY9-VckUR*d");
        conf.put("publisherKey", "publishers:*");
        conf.put("xsltRootPath", "C:\\Users\\Mick\\Documents\\Projects\\FeedHub\\storm_feeds\\src\\xslt\\");

        PublisherSpout publisherSpout = new PublisherSpout();
        publisherSpout.linear(conf);
    }
}
package com.DFM.FeedHub.Storm.Spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.DFM.Client.RedisClient;
import com.DFM.FeedHub.Storm.Bolt.PublisherBolt;
import com.DFM.Util.LinearControl;
import com.DFM.Util.StormUtil;
import com.DFM.Util.StringUtil;

import java.util.*;


public class PublisherSpout extends BaseRichSpout {
    private static final long serialVersionUID = 1L;
    private static String _mode = "unknown";
    private static SpoutOutputCollector _spoutOutputCollector;
    private static String _publisherKey = "";
    private static String _hostname = "Unknown";
    private static RedisClient _redisClient = new RedisClient();

    private static Fields _fields = new Fields("pubKey");
    private static Values _values;
    private static Map<String, Object> _mainConf;

    private static void run() throws Exception {
        String msgId = "not-set" + ":" + _hostname + ":" + System.nanoTime();
        try {
            String pubKey = "";
            byte[] serRedisClient = StormUtil.serialize(_redisClient);
            ArrayList<String> sites = _redisClient.keys(_publisherKey);

            for (String site : sites) {
                pubKey = site;
                msgId = pubKey + ":" + _hostname + ":" + System.nanoTime();
                if (2 == 9) System.out.println("PublisherSpout->Spout START for msgId" + msgId);
                emit(pubKey, msgId);
            }
            //Thread.sleep(60000);
        } catch (Exception e) {
            String msg = "Spout(" + msgId + ");";
            StormUtil.logError(msg, e, _redisClient);
        }
    }

    private static void check() throws Exception {
        _publisherKey = "delivered:WordPress:Wire:AP:content:external:urn:publicid:ap.org:*";
        String msgId = "not-set" + ":" + _hostname + ":" + System.nanoTime();
        try {
            String pubKey = "";
            ArrayList<String> delivered = _redisClient.keys(_publisherKey);
            Iterator<String> DeliveryIterator = delivered.iterator();

            int i = 0;
            Map<String, String> delMap = new HashMap<String, String>();
            Map<String, String> fullMap = new HashMap<String, String>();

            while (DeliveryIterator.hasNext()) {
                i += 1;
                pubKey = DeliveryIterator.next();
                msgId = pubKey + ":" + _hostname + ":" + System.nanoTime();
                if (2 == 9) System.out.println("PublisherSpout->Spout START for msgId" + msgId);
                delMap = _redisClient.hgetAll(pubKey);
                System.out.println(i + " - id: " + delMap.get("id") + " pubkey: " + pubKey + " title: " + delMap.get("title"));
                fullMap.put(Integer.toString(i), delMap.get("id") + ", pubkey: " + pubKey + " title:" + delMap.get("title"));
            }
            Map<String, String> sortMap = sortHashMapByValuesD((HashMap) fullMap);
            printMap(sortMap);
        } catch (Exception e) {
            String msg = "Spout(" + msgId + ");";
            StormUtil.logError(msg, e, _redisClient);
        }
    }

    public static void printMap(Map<String, String> map) {
        Set s = map.entrySet();
        Iterator it = s.iterator();
        String prev = "";
        String flag = "";
        while (it.hasNext()) {
            Map.Entry entry = (Map.Entry) it.next();
            String key = (String) entry.getKey();
            String value = (String) entry.getValue();
            //System.out.println(key + " => " + value);
            if (value.substring(0, 5).equalsIgnoreCase(prev)) {
                flag = "****\r\n";
            } else {
                flag = "";
            }
            System.out.println(flag + value);
            prev = value.substring(0, 5);

        }//while
        System.out.println("========================");
    }//printMap

    public static LinkedHashMap sortHashMapByValuesD(HashMap passedMap) {
        List mapKeys = new ArrayList(passedMap.keySet());
        List mapValues = new ArrayList(passedMap.values());
        Collections.sort(mapValues);
        Collections.sort(mapKeys);

        LinkedHashMap sortedMap = new LinkedHashMap();

        for (Object val : mapValues) {
            for (Object key : mapKeys) {
                String comp1 = passedMap.get(key).toString();
                String comp2 = val.toString();

                if (comp1.equals(comp2)) {
                    passedMap.remove(key);
                    mapKeys.remove(key);
                    sortedMap.put(key, val);
                    break;
                }

            }

        }
        return sortedMap;
    }

    private static void emit(String pubKey, String msgId) {
        _values = new Values(pubKey);
        if (_mode.equalsIgnoreCase("linear")) {
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _mainConf);
            outConf.put("msgId", msgId);
            PublisherBolt.linear(outConf);
        } else {
            _spoutOutputCollector.emit(_values, msgId);
        }
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector spoutOutputCollector) {
        _mode = "live_local";
        configure(conf);
        // Open the spout
        _spoutOutputCollector = spoutOutputCollector;
    }

    public void linear(Map conf) {
        _mode = "linear";
        configure(conf);
        nextTuple();
    }

    private void configure(Map conf) {
        _mainConf = conf;
        _redisClient = new RedisClient(conf);
        _publisherKey = (String) conf.get("publisherKey");
        _hostname = StringUtil.getHostname();
    }

    public void nextTuple() {
        try {
            run();
            //	check();
        } catch (Exception e) {
            e.printStackTrace();
            StormUtil.logError(e, _redisClient);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // create the tuple with field names for Site
        declarer.declare(_fields);
    }

    public void ack(Object msgId) {
        if (1 == 9) System.out.println("Spout ACK for msgId " + msgId);
    }

    public void fail(Object msgId) {
        //if (1==9) System.out.println("Spout FAIL for msgId" + msgId);
        //StormUtil.logError(msgId.toString(), redisClient);
    }
}
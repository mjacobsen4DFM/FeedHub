package com.DFM.FeedHub.Storm.Bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.DFM.Client.RedisClient;
import com.DFM.Model.Helper.Publisher;
import com.DFM.Util.LinearControl;
import com.DFM.Util.StormUtil;

import java.util.HashMap;
import java.util.Map;

public class PublisherBolt extends BaseRichBolt {
    private static final long serialVersionUID = 1L;

    private static String _pubKey = "";
    private static RedisClient _redisClient = new RedisClient();
    private static Publisher _publisher = new Publisher();
    private static Map<String, String> _keys = new HashMap<String, String>();

    private static Fields _fields = new Fields("publisher");
    private static Values _values;
    private static Tuple _tuple;
    private static Map<String, Object> _stormConf;
    private static Map<String, Object> _linearConf;
    private static boolean _isLinear = false;
    private static OutputCollector _collector;

    private static void configure(Map conf) throws Exception {
        _linearConf = conf;
        _isLinear = true;
        Map<String, String> mainConf = (Map) _linearConf.get("mainConf");
        _redisClient = new RedisClient(mainConf);
        // Get the fields from input hashmap.
        _pubKey = (String) _linearConf.get("pubKey");
    }

    private static void run() throws Exception {
        if (2 == 12) System.out.println("PublisherBolt->pubKey: " + _pubKey);
        _keys = _redisClient.hgetAll(_pubKey);
        _keys.put("pubKey", _pubKey);
        _publisher = new Publisher(_keys);
        byte[] binaryPublisher = StormUtil.serialize(_publisher);
        emit(binaryPublisher);
    }

    private static void emit(byte[] binaryPublisher) {
        if (_isLinear) {
            _values = new Values((byte[]) binaryPublisher);
            Map<String, Object> outConf = LinearControl.buildConf(_fields, _values);
            outConf.put("mainConf", _linearConf.get("mainConf"));
            FeedLoadBolt.linear(outConf);
        } else if (_tuple != null) {
            _values = new Values((byte[]) binaryPublisher);
            _collector.emit(_tuple, _values);
        } else {
            String msg = "???: both conf and tuple are null";
            StormUtil.logError(msg, _redisClient);
        }
    }

    public static void linear(Map inConf) {
        try {
            configure(inConf);
            run();
        } catch (Exception e) {
            StormUtil.logError(e, _redisClient);
        }
    }

    @SuppressWarnings("rawtypes")
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _stormConf = conf;
        _collector = collector;
    }

    private void configure(Tuple tuple) throws Exception {
        _tuple = tuple;
        _isLinear = false;
        // Get the fields from input tuple.
        _redisClient = new RedisClient(_stormConf);
        _pubKey = _tuple.getStringByField("pubKey");
    }

    public void execute(Tuple tuple) {
        try {
            configure(tuple);
            run();
            _collector.ack(tuple);
        } catch (Exception e) {
            StormUtil.logError(tuple, e, _redisClient);
            _collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(_fields);
    }
}

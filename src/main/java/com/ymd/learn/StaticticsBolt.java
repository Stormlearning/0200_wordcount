package com.ymd.learn;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class StaticticsBolt implements IBasicBolt{

	private static final long serialVersionUID = 2796757409715967028L;
	
	Map<String, Integer> dataMap = new HashMap<String, Integer>();
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
		String word = input.getStringByField("word");
		Integer count = input.getIntegerByField("count");
		dataMap.put(word, count);
		int totalNum = dataMap.values().stream().mapToInt(i -> i).sum();
		System.err.println("Total num --------------------" + totalNum);
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}

}

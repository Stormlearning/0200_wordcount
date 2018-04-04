package com.ymd.learn;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class WordcountBolt implements IRichBolt {

	private static final long serialVersionUID = 5613666389314323863L;
	
	OutputCollector collector;
	
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
	}
	
	Map<String, Integer> wordcountMap = new HashMap<String, Integer>();

	public void execute(Tuple input) {
		String word = input.getStringByField("word");
		if(!wordcountMap.containsKey(word)) {
			wordcountMap.put(word, 0);
		}
		wordcountMap.put(word, wordcountMap.get(word)+1);
		collector.emit(new Values(word, wordcountMap.get(word)));
		System.err.println("word = "+ word + "; count = " + wordcountMap.get(word));
	}

	public void cleanup() {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

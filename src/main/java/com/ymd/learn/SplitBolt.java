package com.ymd.learn;

import java.util.Map;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.IBasicBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class SplitBolt implements IBasicBolt {

	private static final long serialVersionUID = -2059409347703029319L;

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	public void prepare(Map stormConf, TopologyContext context) {
	}
	
	private String separator;
	
	public SplitBolt(String separator) {
		this.separator = separator;
	}
	
	public void execute(Tuple input, BasicOutputCollector collector) {
		String data = input.getStringByField("data");
		if(data != null) {
			String[] words = data.split(separator);
		    for(String word : words) {
		    	collector.emit(new Values(word));
		    }
		}
	}

	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
	
	
	
	
}

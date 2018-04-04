package com.ymd.learn;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class SourceSpout implements IRichSpout {

	private static final long serialVersionUID = -3489173380127386514L;

	SpoutOutputCollector collector;
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	public void close() {
		
	}

	public void activate() {
		
	}

	public void deactivate() {
		
	}

	public void nextTuple() {
		String[] sourceData = {"a b c d e", "b d e f g", "a d c g d", "e e f f a"};
		for(String data : sourceData) {
			collector.emit(new Values(data));
		}
		try {
			Thread.sleep(60*1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public void ack(Object msgId) {
		
	}

	public void fail(Object msgId) {
		
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("data"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

}

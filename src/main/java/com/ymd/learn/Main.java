package com.ymd.learn;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

public class Main {
	
	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("sourceSpout", new SourceSpout());
		builder.setBolt("splitBolt", new SplitBolt(" "), 4).shuffleGrouping("sourceSpout");
		builder.setBolt("wordcountBolt", new WordcountBolt(), 4).fieldsGrouping("splitBolt", new Fields("word"));
		builder.setBolt("staticticsBolt", new StaticticsBolt(), 1).shuffleGrouping("wordcountBolt");
		
		
		Config conf = new Config();
		conf.setDebug(true);
		
		if(args.length > 0) {
			try {
				StormSubmitter.submitTopology("wordcount", conf, builder.createTopology());
			} catch (AlreadyAliveException e) {
				e.printStackTrace();
			} catch (InvalidTopologyException e) {
				e.printStackTrace();
			} catch (AuthorizationException e) {
				e.printStackTrace();
			}
		} else {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("wordcount", conf, builder.createTopology());
		}
	}
	
}

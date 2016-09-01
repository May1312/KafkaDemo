package com.helian.storm2;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

public class ToKafkaBolt<S1, S2> implements IRichBolt {

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub

	}

	@Override
	public void execute(Tuple paramTuple) {
		// TODO Auto-generated method stub

	}

	@Override
	public void prepare(Map paramMap, TopologyContext paramTopologyContext,
			OutputCollector paramOutputCollector) {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(
			OutputFieldsDeclarer paramOutputFieldsDeclarer) {
		// TODO Auto-generated method stub

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}

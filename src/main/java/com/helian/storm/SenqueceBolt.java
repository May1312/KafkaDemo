package com.helian.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SenqueceBolt extends BaseBasicBolt{
	/**
	 * SenqueceBolt实现很简单，在接收的spout的消息前面加上“I‘m”　
	 */
	private static final long serialVersionUID = 1L;

	public void execute(Tuple input, BasicOutputCollector collector) {
         String word = (String) input.getValue(0);  
         String out = "I'm " + word +  "!";  
         System.out.println("out=" + out);
         collector.emit(new Values(out));
    }
    
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("message"));
    }
}

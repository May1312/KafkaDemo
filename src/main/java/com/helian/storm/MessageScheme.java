package com.helian.storm;

import java.io.UnsupportedEncodingException;
import java.util.List;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
/**
 * 使用KafkaSpout时需要子集实现Scheme接口，它主要负责从消息流中解析出需要的数据
 * @author An
 *
 */
public class MessageScheme implements Scheme {
	
	private static final long serialVersionUID = -7346046274720549570L;

	public List<Object> deserialize(byte[] ser) {
        try {
            String msg = new String(ser, "UTF-8"); 
            return new Values(msg);
        } catch (UnsupportedEncodingException e) {  
         
        }
        return null;
    }
    
    public Fields getOutputFields() {
        // TODO Auto-generated method stub
        return new Fields("msg");  
    }  
}

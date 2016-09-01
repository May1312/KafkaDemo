package com.helian.kafka;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

public class Producertest {
	public static void main(String[] args) {
        Properties props = new Properties();
        props.put("zk.connect", "192.168.44.133:2181,192.168.44.129:2181");
        // serializer.class为消息的序列化类(配置value的序列化类)
        props.put("serializer.class", "kafka.serializer.StringEncoder");
        //配置key的序列化类
        props.put("key.serializer.class", "kafka.serializer.StringEncoder");
        // 配置metadata.broker.list, 为了高可用, 最好配两个broker实例
        props.put("metadata.broker.list", "192.168.44.133:9092,192.168.44.129:9092");
        // 设置Partition类, 对队列进行合理的划分
        //props.put("partitioner.class", "idoall.testkafka.Partitionertest");
        // ACK机制, 消息发送需要kafka服务端确认
        props.put("request.required.acks", "1");

         props.put("num.partitions", "6");
        ProducerConfig config = new ProducerConfig(props);
        Producer<String, String> producer = new Producer<String, String>(config);
        //for (int i = 0; i < 2; i++)
      //  {
          // KeyedMessage<K, V>
          // 　　K对应Partition Key的类型
          // 　　V对应消息本身的类型
//　 topic: "test", key: "key", message: "message"
          SimpleDateFormat formatter = new SimpleDateFormat   ("yyyy年MM月dd日 HH:mm:ss SSS");      
          Date curDate = new Date(System.currentTimeMillis());//获取当前时间      
          String str = formatter.format(curDate);   
           
          String msg = "java-测试-kafka 当世界都一样"+"="+str;
          KeyedMessage<String, String> keyedMessage= new KeyedMessage<String, String>("test", "key?", msg);
          producer.send(keyedMessage);
       // }
      }
}

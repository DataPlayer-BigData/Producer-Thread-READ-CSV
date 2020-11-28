package tu.cit.examples.kafkaapi;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class ProducerThreadDemo {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String[] args) throws InterruptedException {

        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"my-producer-threading-demo");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(props);
        Thread[] dispachers = new Thread[3];
        logger.info("Starting Dispacher threads...");

        String dataFiles[] = {"data/student.csv","data/student1.csv","data/student2.csv"};
        String topicName = "studentThread";
        for(int i=0;i<3;i++){
            dispachers[i] = new Thread( new StudentThread(producer,topicName,dataFiles[i]));
            dispachers[i].start();
            dispachers[i].sleep(10000);
        }

        try{
            for(Thread t : dispachers) t.join();
        }catch(InterruptedException e){
            logger.error("Main Thread Interrupted");
        }finally{
            producer.close();
            logger.info("Finished Producer Thread Demo.");
        }

    }
}

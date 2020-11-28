package tu.cit.examples.kafkaapi;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

public class StudentThread implements Runnable{
    private static final Logger logger = LogManager.getLogger();
    private String fileLocation;
    private String topicName;
    private KafkaProducer<String,String> producer;

    StudentThread(KafkaProducer<String,String> producer, String topicName, String fileLocation){
        this.fileLocation = fileLocation;
        this.topicName = topicName;
        this.producer = producer;
    }
    @Override
    public void run() {
        logger.info("Start Processing " + fileLocation);
        File file = new File(fileLocation);
        int counter=0;

        try(Scanner scanner = new Scanner(file)){
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName,null,line));
                counter++;
            }
            logger.info("Finished Sending " + counter + "messages from " + fileLocation);
        }catch(FileNotFoundException e){
            throw new RuntimeException(e);
        }
    }
}

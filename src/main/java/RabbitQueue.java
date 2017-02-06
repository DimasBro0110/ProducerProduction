import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.net.SyslogAppender;
import org.json.simple.JSONObject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import static java.nio.file.StandardWatchEventKinds.ENTRY_CREATE;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Created by Dmitry on 02.02.2017.
 */
public class RabbitQueue {

    private final String queueFileMonitorService = "FileServiceMQ";
    private WatchService watchService = null;
    static Logger log = Logger.getLogger(RabbitQueue.class.getName());
    private ConnectionFactory connectionFactory = new ConnectionFactory();
    private String fs;
    private Schema avroSchema;
    private Path pathToDirectory;

    RabbitQueue(String host, String port, Path pathToDirectory, String pathToAvro, String fileSystem){
        try {
            this.connectionFactory.setHost(host);
            this.connectionFactory.setPort(Integer.valueOf(port));
            this.connectionFactory.setUsername("guest");
            this.connectionFactory.setPassword("guest");
            this.pathToDirectory = pathToDirectory;
            this.watchService = this.pathToDirectory.getFileSystem().newWatchService();
            this.fs = fileSystem;
            this.avroSchema = new Schema.Parser().parse(new File(pathToAvro));
            pathToDirectory.register(watchService, ENTRY_MODIFY);
            log.log(Level.INFO, "Watch logger registered for: " + pathToDirectory.getFileName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public List<GenericRecord> demarshallFileAvro(File file) throws IOException {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(this.avroSchema);
        DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file, reader);
        GenericRecord record = null;
        List<GenericRecord> lst = new ArrayList<GenericRecord>();
        while(fileReader.hasNext()){
            record = fileReader.next();
            lst.add(record);
        }
        fileReader.close();
        return lst;
    }

    public byte[] getByteFromGenericRecord(List<GenericRecord> lst) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(this.avroSchema);
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try{
            Encoder e = EncoderFactory.get().binaryEncoder(os, null);
            for(GenericRecord record: lst){
                writer.write(record, e);
            }
            e.flush();
            return os.toByteArray();
        }catch (Exception ex){
            return null;
        }finally {
            os.close();
        }
    }

    public void runFilemonitorService(){
        JSONObject jsonObject = new JSONObject();
        try {
            Connection connection = this.connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(this.queueFileMonitorService, true, false, false, null);
            File folder = new File(this.pathToDirectory.toString());
            for(File file: folder.listFiles()){
                log.log(Level.INFO, "Processing path... ");
                jsonObject.put("path", file.getPath());
                jsonObject.put("data", new Timestamp(System.currentTimeMillis()).toString());
                channel.basicPublish("", this.queueFileMonitorService, null, jsonObject.toString().getBytes());
                log.log(Level.INFO, "Processed path! And sent to Rabbit !!!");
            }
        }catch (Exception ex){

        }

    }

}

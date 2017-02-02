import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.json.simple.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import static java.nio.file.StandardWatchEventKinds.ENTRY_MODIFY;

/**
 * Created by Dmitry on 02.02.2017.
 */
public class RabbitQueue {

    private final String queueFileMonitorService = "FileServiceMQ";
    private WatchService watchService = null;
    static Logger log = Logger.getLogger(RabbitQueue.class.getName());
    private ConnectionFactory connectionFactory = new ConnectionFactory();
    private Path pathToDirectory;

    RabbitQueue(String host, String port, Path pathToDirectory){
        try {
            this.connectionFactory.setHost(host);
            this.connectionFactory.setPort(Integer.valueOf(port));
            this.connectionFactory.setUsername("guest");
            this.connectionFactory.setPassword("guest");
            this.pathToDirectory = pathToDirectory;
            this.watchService = this.pathToDirectory.getFileSystem().newWatchService();
            pathToDirectory.register(watchService, ENTRY_MODIFY);
            log.log(Level.INFO, "Watch logger registered for: " + pathToDirectory.getFileName());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void runFilemonitorService(){
        JSONObject jsonObject = new JSONObject();
        try {
            Connection connection = this.connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            channel.queueDeclare(this.queueFileMonitorService, true, false, false, null);
            for(;;){
                WatchKey key = this.watchService.take();
                for(WatchEvent<?> event: key.pollEvents()){
                    WatchEvent.Kind<?> kind = event.kind();
                    @SuppressWarnings("unchecked")
                    WatchEvent<Path> ev = (WatchEvent<Path>) event;
                    Path fileName = ev.context();
                    log.log(Level.INFO, kind.name() + " has occurred with file : " + fileName.toString());
                    boolean valid = key.reset();
                    if (!valid) {
                        break;
                    }
                    String pathAbsolute = this.pathToDirectory.toString() + "\\" + fileName.toString();
                    jsonObject.put("event", kind.name());
                    jsonObject.put("path", pathAbsolute);
                    channel.basicPublish("", this.queueFileMonitorService, null, jsonObject.toJSONString().getBytes());
                }

            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}

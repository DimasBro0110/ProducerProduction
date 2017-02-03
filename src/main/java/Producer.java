import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Dmitry on 02.02.2017.
 */
public class Producer {

    public static void main(String[] args) throws IOException {
        RabbitQueue rabbitQueue = new RabbitQueue("192.168.100.124", "5672", Paths.get("/home/dimas/testec"));
        rabbitQueue.runFilemonitorService();
    }

}

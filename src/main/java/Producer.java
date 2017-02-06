import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

/**
 * Created by Dmitry on 02.02.2017.
 */
public class Producer {

    public static void main(String[] args) throws IOException {
        if(args.length < 4){
            System.out.println("Not Enough Arguments Passed");
            System.exit(1);
        }else {
            String rabbitHost = args[0];
            String rabbitPort = args[1];
            String pathToScan = args[2];
            String pathToAvro = args[3];
            String fs = args[4];
            RabbitQueue rabbitQueue = new RabbitQueue(rabbitHost, rabbitPort, Paths.get(pathToScan), pathToAvro, fs);
            rabbitQueue.runFilemonitorService();
        }
    }

}

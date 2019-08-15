import org.apache.hadoop.conf.Configuration;
import utils.OrcReader;
import utils.ReadTask;
import utils.RuntimeMemoryUtil;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ReaderMain {
    private static Configuration conf = new Configuration();
    public static void main(String[] args) throws Exception {
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        int numOfFilesToRead = Integer.parseInt(args[0]);
        try {
            if (numOfFilesToRead < 1) {
                System.out.println("Please provide at least one file read location.");
            } else if (numOfFilesToRead == 1) {
                System.out.println("Please input file read location: ");
                String location = br.readLine();
                OrcReader.FileReader(location, conf);
            } else {
                String[] locations = new String[numOfFilesToRead];
                for (int i = 0; i < numOfFilesToRead; i++) {
                    System.out.println("Please input file read location " + (i + 1) + " :");
                    locations[i] = br.readLine();
                }
                ExecutorService es =
                        Executors.newFixedThreadPool(Math.min(RuntimeMemoryUtil.getNumberOfCores(), numOfFilesToRead));
                for (String loc : locations) {
                    ReadTask task = new ReadTask(loc, conf);
                    es.submit(task);
                    System.out.println("Executing Thread: " + Thread.currentThread().getName());
                }
                es.shutdown();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

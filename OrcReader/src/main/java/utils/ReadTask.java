package utils;

import org.apache.hadoop.conf.Configuration;

public class ReadTask implements Runnable {
    private String loc;
    private Configuration conf;

    public ReadTask(String loc, Configuration conf) {
        super();
        this.loc = loc;
        this.conf = conf;
    }

    public void run() {
        try {
            OrcReader.FileReader(loc, conf);
            System.out.println("Executing Thread: " + Thread.currentThread().getName());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

import WriterUtils.OrcWriter;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.hadoop.conf.Configuration;

import java.io.File;

public class WriterMain {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        StopWatch stopWatchWrite = new StopWatch();
        stopWatchWrite.start();
        String loc = "/Users/rakesh.das01/orcFile_small";
        String fromDate1 = "2018-01-01";
        String toDate1 = "2018-01-05";
        String fromDate2 = "2018-01-06";
        String toDate2 = "2018-01-10";
        Double fileSizeMax = 900.00;  //File of Size in MB upto 2 decimal place precision
        OrcWriter writer = new OrcWriter();
        File file = new File(loc);
        boolean fileExists = file.exists();
        if(fileExists) {
            file.delete();
            System.out.println("File deleted");
        }
        writer.writeOrcFile(loc,conf,fromDate1, toDate1, fromDate2, toDate2, fileSizeMax);
        System.out.println(loc +" generated");
        stopWatchWrite.stop();
        System.out.println("Time Elapsed in seconds for writing : " + stopWatchWrite.getTime() / 1000.0);
    }
}

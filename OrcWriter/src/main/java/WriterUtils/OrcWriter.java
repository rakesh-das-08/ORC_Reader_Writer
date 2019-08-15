package WriterUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

public  class OrcWriter {
    private static final AtomicInteger id = new AtomicInteger(0);

    private Double checkFileSize(String filelocation) throws FileNotFoundException {
        File file = new File(filelocation);
        double d= (double)file.length()/(1024*1024);
        return Math.round(d * 100.0)/100.0;
    }

    private Date between(Date from, Date to) throws IllegalArgumentException {
        if (to.before(from)) {
            throw new IllegalArgumentException("Invalid date range, the upper bound date is before the lower bound.");
        }
        long offsetMillis = (long) (Math.random() * (to.getTime() - from.getTime()));
        return new Date(from.getTime() + offsetMillis);
    }

    public void writeOrcFile(String fileLocation, Configuration conf, String fromDate1, String toDate1,
                             String fromDate2, String toDate2,Double fileSizeMax) throws Exception {
        try {
                Path path = new Path(fileLocation);
                FileSystem fs = FileSystem.get(conf);

                DateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
                Date from1 = org.apache.commons.lang3.time.DateUtils.parseDate(fromDate1, "yyyy-mm-dd");
                Date to1 = org.apache.commons.lang3.time.DateUtils.parseDate(toDate1, "yyyy-mm-dd");
                Date from2 = org.apache.commons.lang3.time.DateUtils.parseDate(fromDate2, "yyyy-mm-dd");
                Date to2 = org.apache.commons.lang3.time.DateUtils.parseDate(toDate2, "yyyy-mm-dd");
                Date between1 = between(from1, to1);
                Date between2 = between(from2, to2);

                ObjectInspector inspector = ObjectInspectorFactory.getReflectionObjectInspector
                        (SchemaDef.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);

            int rowIndexStride = 10000;
            int bufferSize = 262144;
            long stripeSize = 268435456L;
            //long stripeSize = 134217728L;
            CompressionKind compressionLib = CompressionKind.ZLIB;
            Writer writer = OrcFile.createWriter(fs, path, conf, inspector, stripeSize,
                    compressionLib, bufferSize, rowIndexStride);

                while(checkFileSize(fileLocation)<fileSizeMax) {
                    writer.addRow(
                            new SchemaDef(
                                    (long)id.incrementAndGet(),              //Incremental id
                                    Math.abs(new Random().nextLong()),       //col_1
                                    Math.abs(new Random().nextLong()),       //col_2
                                    Math.abs(new Random().nextLong()),       //col_3
                                    new Text(UUID.randomUUID().toString()),  //col_4
                                    new Text(UUID.randomUUID().toString()),  //col_5
                                    new Text(UUID.randomUUID().toString()),  //col_6
                                    new Text(" "),                           //col_7
                                    new Text(formatter.format(between1)),    //col_8
                                    new Text(formatter.format(between2)),    //col_9
                                    Math.abs(new Random().nextLong()),       //col_10
                                    Math.abs(new Random().nextLong())        //col_11
                            )
                    );
                }
                System.out.println("No of rows : "+writer.getNumberOfRows());
                System.out.println("ORC file of size " + checkFileSize(fileLocation) + "MB generated at " + fileLocation);
                writer.close();
        } catch(Exception e){
            e.printStackTrace();
        }
    }
}

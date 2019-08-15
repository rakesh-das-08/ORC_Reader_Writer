package utils;

import au.com.bytecode.opencsv.CSVWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

import java.io.*;
import java.util.List;

public class OrcReader {
    /**
     * 
     * @param inputFileLocation
     * @param conf
     * @throws Exception
     */
    public static void FileReader(String inputFileLocation, Configuration conf) throws Exception {
        try {
            conf.setBoolean("orc.use.zerocopy",true);
            Path path = new Path(inputFileLocation);
            OrcFile.ReaderOptions readerOptions = OrcFile.readerOptions(conf);
            Reader reader = OrcFile.createReader(path, readerOptions);
            StructObjectInspector inspector = (StructObjectInspector) reader.getObjectInspector();
            System.out.println("Compression Size: " + reader.getCompressionSize());
            RecordReader records = reader.rows();
            Object row = null;
            List fields = inspector.getAllStructFieldRefs();
            for (Object field : fields) {
                System.out.print(((StructField) field).getFieldObjectInspector().getTypeName() + '\t');
            }
            System.out.println("\n");
            while (records.hasNext()) {
                row = records.next(row);
                List value_lst = inspector.getStructFieldsDataAsList(row);
                StringBuilder builder = new StringBuilder();
                for (Object field : value_lst) {
                    if (field != null)
                        builder.append(field.toString());
                    builder.append('\t');
                }
                System.out.println(builder.toString());
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

package WriterUtils;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/*
 * This class defines the schema of the ORC file you want to generate.
 * It aslo has some metadata methods.
 * Please change it as per your schema requirements*/

@SuppressWarnings( {"UnusedDeclaration"})
public class SchemaDef {
    private final LongWritable id;
    private final LongWritable col_1;
    private final LongWritable col_2;
    private final LongWritable col_3;
    private final Text col_4;
    private final Text col_5;
    private final Text col_6;
    private final Text col_7;
    private final Text col_8;
    private final Text col_9;
    private final LongWritable  col_10;
    private final LongWritable  col_11;

    SchemaDef(Long id, Long col_1, Long col_2, Long col_3, Text col_4, Text col_5, Text col_6,
                         Text col_7, Text col_8, Text col_9,
                         Long col_10, Long col_11) {
        this.id = new LongWritable(id);
        this.col_1 = new LongWritable(col_1);
        this.col_2 = new LongWritable(col_2);
        this.col_3 = new LongWritable(col_3);
        this.col_4 = col_4;
        this.col_5 = col_5;
        this.col_6 = col_6;
        this.etl_action_cd = col_7;
        this.etl_create_ts = col_8;
        this.etl_change_ts = col_9;
        this.etl_create_batch_sk = new LongWritable(col_10);
        this.etl_change_batch_sk = new LongWritable(col_11);
    }

    @Override
    public String toString() {
        return "{" +
                id +
                ", " +col_1+
                ", " +col_2+
                ", " +col_3+
                ", " +col_4+
                ", " +col_5+
                ", " +col_6+
                ", " +col_7+
                ", " +col_8+
                ", " +col_9+
                ", " +col_10+
                ", " +col_11+
                '}';
    }

    public static String[] getFieldNames() {
        return new String[] {
                "id",
                "col_1",
                "col_2",
                "col_3",
                "col_4",
                "col_5",
                "col_6",
                "col_7",
                "col_8",
                "col_9",
                "col_10",
                "col_11"
        };
    }

    public static String[] getFieldTypes() {
        return new String[] {
                "bigint",
                "bigint",
                "bigint",
                "bigint",
                "string",
                "string",
                "string",
                "string",
                "string",
                "string",
                "bigint",
                "bigint"
        };
    }

    public static String[] getKeyNames() {
        return new String[] {
                "id"
        };
    }
}

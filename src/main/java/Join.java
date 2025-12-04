
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Join {
    private static final String INPUT_PATH = "input-join/";
    private static final String OUTPUT_PATH = "output/join-";
    private static final Logger LOG = Logger.getLogger(Join.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new SimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        private final static String emptyWords[] = { "" };

        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split("[|]");

            String joinkey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords)) {
                return;
            }

            if (fileName.equals("customers.tbl")) {
                joinkey = words[0];
                elem = words[1];
            } else {
                joinkey = words[1];
                elem = words[8];
            }

            context.write(new Text(joinkey), new Text(elem));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int i = 0;

            String res = "";

            String val1 = "";
            String val2 = "";


            for (Text val : values) {
                if (i == 0) {
                    val1 = val.toString();
                    i++;
                } else if (i == 1){
                    val2 = val.toString();
                    i++;
                }
            }

            //Check si y'a un couple
            if (i < 2){
                return;
            }

            //Check si c'est bien un couple customer-order
            if(val1.contains("Cust")){
                if(val2.contains("Cust")){
                    return; // couple customer-customer
                }
            }
            else if(!val2.contains("Cust")){
                return; // couple order-order
            } else { //inverse l'ordre des attributs pour le mettre correctement
                String temp = val1;
                val1 = val2;
                val2 = temp;
            }

            //

            res = "| "+val1 + " | " + val2+ " |";
            key = new Text("");
            context.write(key, new Text(res));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Join");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}
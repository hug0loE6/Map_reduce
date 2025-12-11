
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class Request4 {
    private static final String INPUT_PATH = "input-trans/";
    private static final String OUTPUT_PATH = "output/trans4-";
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
        private final static String headerAtt[] = {"ID_CONTRAT", "ID_PRODUCTEUR", "ID_DISTRIBUTEUR", "ID_DATE_DEBUT",
                "ID_DATE_FIN", "PRIX_UNITAIRE", "VOLUME_PREVUE", "MONTANT_CONTRAT" , "DATE_SIGNATURE" };

        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            
            if(!fileName.equals("CONTRAT_ACHAT.csv")){
                return;
            }

            String line = value.toString();
            String[] words = line.split(",");

            String writekey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords) || Arrays.equals(words, headerAtt)) {
                return;
            }

            String[] datesplit = words[8].split("-");
            writekey = datesplit[0];

            context.write(new Text(writekey), new Text(elem));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, IntWritable> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int count = 0;

            for (Text val : values) {
                count++;
            }
            context.write(key, new IntWritable(count));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "Req4");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job.waitForCompletion(true);
    }
}
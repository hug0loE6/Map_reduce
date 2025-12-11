
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
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

public class Request5 {
    private static final String INPUT_PATH = "input-trans/";
    private static final String OUTPUT_PATH = "output/trans5-";
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

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        private final static String emptyWords[] = { "" };
        private final static String headerAttAchat[] = {"ID_CONTRAT", "ID_PRODUCTEUR", "ID_DISTRIBUTEUR", "ID_DATE_DEBUT",
                "ID_DATE_FIN", "PRIX_UNITAIRE", "VOLUME_PREVUE", "MONTANT_CONTRAT" , "DATE_SIGNATURE" };
        private final static String headerAttProd[] = {"ID_PRODUCTEUR","NOM","TYPE_PRODUCTEUR","PAYS","REGION",
                "CODE_POSTAL","DATE_COLLABORATION","DATE_CREATION","EMAIL","ACTIF","EMISSIONS"};

        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] words = line.split(",");

            String writekey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords) || Arrays.equals(words, headerAttAchat) || Arrays.equals(words, headerAttProd)) {
                return;
            }

            if (fileName.equals("CONTRAT_ACHAT.csv")){
                writekey = words[1];
                elem = "A|" + words[0] + "," + words[1] + "," + words[2] + "," + words[3] + "," + words[4];
                context.write(new Text(writekey), new Text(elem));
            }
            else if (fileName.equals("PRODUCTEUR.csv")){
                writekey = words[0];
                elem = "P|" + words[2];
                context.write(new Text(writekey), new Text(elem));
            }     
        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String prod = "";
            boolean foundProd = false;
            for (Text val : values) {
                String[] parts = val.toString().split("[|]");
                if (parts[0].equals("P")) {
                    prod = parts[1];
                    foundProd = true;
                } 
            }
            if (!foundProd) {
                return;
            }

            for (Text val : values) {
                String[] parts = val.toString().split("[|]");
                if (parts[0].equals("A")){
                    String res = parts[1] + "," + prod;
                    context.write(new Text(""), new Text(res));
                }
            }
        }
    }



    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        private final static String emptyWords[] = { "" };
        private final static String headerAttDate[] = {"ID_DATE","JOUR","JOUR_SEMAINE","MOIS","ANNEE","SAISON","TYPE_JOUR"};

        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split(",");

            String writekey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords) || Arrays.equals(words, headerAttDate)) {
                return;
            }

            if (fileName.equals("DIM_DATE.csv")){
                writekey = words[0];
                elem = "P|" + words[4] + "-" + words[3] + "-" + words[1];
                context.write(new Text(writekey), new Text(elem));
            }
            else if (fileName.equals("trans2-job1")){
                writekey = words[3];
                elem = "A|" + line;
                context.write(new Text(writekey), new Text(elem));
            }     
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String prod = "";
            boolean foundProd = false;
            for (Text val : values) {
                String[] parts = val.toString().split("[|]");
                if (parts[0].equals("P")) {
                    prod = parts[1];
                    foundProd = true;
                } 
            }
            if (!foundProd) {
                return;
            }

            for (Text val : values) {
                String[] parts = val.toString().split("[|]");
                if (parts[0].equals("A")){
                    String res = parts[1] + "," + prod;
                    context.write(new Text(""), new Text(res));
                }
            }
        }
    }

    public static class Map3 extends Mapper<LongWritable, Text, Text, Text> {
        private final static String emptyWords[] = { "" };
        private final static String headerAttDate[] = {"ID_DATE","JOUR","JOUR_SEMAINE","MOIS","ANNEE","SAISON","TYPE_JOUR"};

        private String fileName;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            fileName = fileSplit.getPath().getName();
        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split(",");

            String writekey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords) || Arrays.equals(words, headerAttDate)) {
                return;
            }

            if (fileName.equals("DIM_DATE.csv")){
                writekey = words[0];
                elem = "P|" + words[4] + "-" + words[3] + "-" + words[1];
                context.write(new Text(writekey), new Text(elem));
            }
            else if (fileName.equals("trans2-job2")){
                writekey = words[4];
                elem = "A|" + line;
                context.write(new Text(writekey), new Text(elem));
            }     
        }
    }

    public static class Reduce3 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String prod = "";
            boolean foundProd = false;
            for (Text val : values) {
                String[] parts = val.toString().split("[|]");
                if (parts[0].equals("P")) {
                    prod = parts[1];
                    foundProd = true;
                } 
            }
            if (!foundProd) {
                return;
            }

            for (Text val : values) {
                String[] parts = val.toString().split("[|]");
                if (parts[0].equals("A")){
                    String res = parts[1] + "," + prod;
                    context.write(new Text(""), new Text(res));
                }
            }
        }
    }

    public static class Map4 extends Mapper<LongWritable, Text, Text, LongWritable> {
        private final static String emptyWords[] = { "" };
 //ID_CONTRAT, ID_PRODUCTEUR, ID_DISTRIBUTEUR, ID_DATE_DEBUT, ID_DATE_FIN, type_producteur, date_debut, date_fin
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            String[] words = line.split(",");

            String writekey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords)) {
                return;
            }

            writekey = words[5];
            LocalDate dateDeb = LocalDate.parse(words[6]);
            LocalDate dateFin = LocalDate.parse(words[7]);
            long days = Math.abs(ChronoUnit.DAYS.between(dateDeb, dateFin));
            context.write(new Text(writekey), new LongWritable(days));


        }
    }

    public static class Reduce4 extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long total = 0;
            int count = 0;
            for (LongWritable val : values) {
                total += val.get();
                count += 1;
            }
            long avg = total / count;
            context.write(key, new LongWritable(avg));
        }
    }



    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = new Job(conf, "joinAchatProd");

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH + "-job1"));

        job1.waitForCompletion(true);

        Job job2 = new Job(conf, "JoinAPDateDeb");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(INPUT_PATH + "DIM_DATE.csv"));
        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH + "-job1"));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH + "-job2"));

        job2.waitForCompletion(true);

        Job job3 = new Job(conf, "JoinAPDateFin");

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(LongWritable.class);

        job3.setMapperClass(Map3.class);
        job3.setReducerClass(Reduce3.class);

        job3.setInputFormatClass(TextInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job3, new Path(INPUT_PATH + "DIM_DATE.csv"));
        FileInputFormat.addInputPath(job3, new Path(OUTPUT_PATH + "-job2"));
        FileOutputFormat.setOutputPath(job3, new Path(OUTPUT_PATH + "-job3"));

        job3.waitForCompletion(true);

        Job job4 = new Job(conf, "Req5");

        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        job4.setMapperClass(Map4.class);
        job4.setReducerClass(Reduce4.class);

        job4.setInputFormatClass(TextInputFormat.class);
        job4.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job4, new Path(OUTPUT_PATH + "-job3"));
        FileOutputFormat.setOutputPath(job4, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job4.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(OUTPUT_PATH + "-job1"), true);
        fs.delete(new Path(OUTPUT_PATH + "-job2"), true);
        fs.delete(new Path(OUTPUT_PATH + "-job3"), true);
    }
}

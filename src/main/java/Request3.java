
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

public class Request3 {
    private static final String INPUT_PATH = "input-snap/";
    private static final String OUTPUT_PATH = "output/snap3-";
    private static final Logger LOG = Logger.getLogger(Join.class.getName());

    static {
        System.setProperty("java.util.logging.SimpleFormatter.format", "%5$s%n%6$s");

        try {
            FileHandler fh = new FileHandler("out.log");
            fh.setFormatter(new Ssrc/main/java/Request4.javaimpleFormatter());
            LOG.addHandler(fh);
        } catch (SecurityException | IOException e) {
            System.exit(1);
        }
    }

    public String profil(String type, String cadran){
        if(type.equals("Résidentiel")){
            if(cadran.equals("Base")){
                return "RES1";
            } else if (cadran.equals("HP/HC")){
                return "RES2";
            }
        } else if (type.equals("Professionnel")){
            if(cadran.equals("Base")){
                return "PRO1";
            } else if (cadran.equals("HP/HC")){
                return "PRO2";
            }
        } else if (type.equals("Entreprise")){
            if(cadran.equals("Base")){
                return "ENT1";
            } else if (cadran.equals("HP/HC")){
                return "ENT2";
            } else if (cadran.equals("HPH/HPE/HCH/HCE")){
                return "ENT4";
            }
        }
        return "ERR";
    }

    public static class Map1 extends Mapper<LongWritable, Text, Text, Text> {
        private final static String emptyWords[] = { "" };
        private final static String headerAttConso[] = {"ID_CONTRACT","ID_CONF","ID_CLIENT","ID_DATE","MONTANT_FACTURÉ",
                "CONSOMMATION","CONSOMMATION_CUMULÉE","JOURS_AVANT_FIN_CONTRAT"};
        private final static String headerAttConf[] = {"ID_CONF","POINT_DE_LIVRAISON","TYPE_DE_PROFIL","CADRANS",
                "DATE_DÉBUT_CONFIGURATION","DATE_FIN_CONFIGURATION","PUISSANCE_SOUSCRITE","ADRESSE"};

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

            if (Arrays.equals(words, emptyWords) || Arrays.equals(words, headerAttConf) || Arrays.equals(words, headerAttConso)) {
                return;
            }

            if (fileName.equals("CONSO_REEL.csv")) {
                writekey = words[1]; 
                elem = "A|" + words[0]; 
                context.write(new Text(writekey), new Text(elem));
            } else if (fileName.equals("CONFIGURATION_TECHNIQUE.csv")) {
                writekey = words[0];
                elem = "P|" + profil(words[2], words[3]);
                context.write(new Text(writekey), new Text(elem)); 
            }
        }
    }

    public static class Reduce1 extends Reducer<Text, Text, Text, Text> {

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            ArrayList<String> distinct = new ArrayList<>();
            
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
                if (parts[0].equals("A") && !distinct.contains(parts[1])){
                    String res = parts[1] + "," + prod;
                    distinct.add(parts[1]);
                    context.write(new Text(""), new Text(res));
                }
            }

        }
    }


    public static class Map2 extends Mapper<LongWritable, Text, Text, Text> {
        //id_contrat,Profil
        private final static String emptyWords[] = { "" };
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String line = value.toString().trim();
            String[] words = line.split(",");

            String writekey = "";
            String elem = "";

            if (Arrays.equals(words, emptyWords)) {
                return;
            } else if (words[1].equals("ERR")){
                return;
           }

            writekey = words[1];
            elem = words[0];    

            context.write(new Text(writekey), new Text(elem));
        }
    }

    public static class Reduce2 extends Reducer<Text, Text, Text, LongWritable> {

        private long coult;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            count = Files.lines(Paths.get(OUTPUT_PATH + "-job1")).count();
        }
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            
            int nbPro = 0;
            for (Text val : values) {
                nbPro++;
            }
            Long partrelatif = (nbPro*100) / count;
            context.write(key, new LongWritable(partrelatif));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job1 = new Job(conf, "join");

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        job1.setMapperClass(Map1.class);
        job1.setReducerClass(Reduce1.class);

        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job1, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job1, new Path(OUTPUT_PATH + "-job1"));

        job1.waitForCompletion(true);

        Job job2 = new Job(conf, "Req3");

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(LongWritable.class);

        job2.setMapperClass(Map2.class);
        job2.setReducerClass(Reduce2.class);

        job2.setInputFormatClass(TextInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job2, new Path(OUTPUT_PATH + "-job1"));
        FileOutputFormat.setOutputPath(job2, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

        job2.waitForCompletion(true);

        FileSystem fs = FileSystem.get(conf);
        fs.delete(new Path(OUTPUT_PATH + "-job1"), true);
    }
}
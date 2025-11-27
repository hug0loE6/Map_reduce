import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class GroupByOrder {
	private static final String INPUT_PATH = "input-groupBy/";
	private static final String OUTPUT_PATH = "output/groupBy-";
	private static final Logger LOG = Logger.getLogger(GroupByOrder.class.getName());

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
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static String emptyWords[] = { "" };
        private final static String attributes[] = {"Row ID","Order ID",
                "Order Date","Ship Date","Ship Mode","Customer ID",
                "Customer Name","Segment","Country","City","State",
                "Postal Code","Region","Product ID","Category","Sub-Category",
                "Product Name","Sales","Quantity","Discount","Profit"};
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
            String[] words = line.split(",");


            if (Arrays.equals(words, emptyWords) ||  Arrays.equals(words, attributes)) {
                return;
            }

            String OrderId = words[1];
            int quantity =  Integer.parseInt(words[18]);

            context.write(new Text(OrderId), new IntWritable(quantity));
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
            int count = 0;
            int sumQuantity = 0;

            for (IntWritable val : values) {
                sumQuantity += val.get();
                count++;
            }

            String sumPlusCount = count + "  " +  sumQuantity;

            context.write(key, new Text(sumPlusCount));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();

		Job job = new Job(conf, "GroupBy");

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputValueClass(IntWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
		FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH + Instant.now().getEpochSecond()));

		job.waitForCompletion(true);
	}
}
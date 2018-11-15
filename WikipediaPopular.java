import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Pattern;
import static java.nio.charset.StandardCharsets.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;

public class WikipediaPopular extends Configured implements Tool {

        public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

                private final static IntWritable visits = new IntWritable();
                private Text word = new Text();
                private ArrayList<String> timelist = new ArrayList<String>();

                @Override
                public void map(LongWritable key, Text value, Context context
                                ) throws IOException, InterruptedException {


                        Pattern word_sep = Pattern.compile("\\s");
                        String[] stringarray = word_sep.split(value.toString());
                        String timelist = new String();
                        String lang = new String();
                        String title = new String();
                        int novisit = 0;
                        long bytesize = 0;

                        timelist = stringarray[0];
                        lang = stringarray[1];
                        title = stringarray[2];
                        novisit = Integer.parseInt(stringarray[3]);
                        bytesize = Long.parseLong(stringarray[4]);

                        if(lang.startsWith("en"))
                        {
                                if(!title.equals("Main_Page") && !title.startsWith("Special:"))
                                {

                                        word.set(timelist);
                                        visits.set(novisit);
                                        context.write(word,visits);
                                }
                        }

                }

        }


        public static class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
                private IntWritable result = new IntWritable();

                @Override
                public void reduce(Text key, Iterable<IntWritable> values,
                                Context context
                                ) throws IOException, InterruptedException {
                        int max = 0;
                        for (IntWritable val : values) {
                                if(val.get() > max){
                                        max = val.get();
                                }

                        }
                        result.set(max);
                        context.write(key, result);
                }
        }


        public static void main (String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new WikipediaPopular(), args);
                System.exit(res);
        }

        @Override
        public int run(String[] args) throws Exception {
                Configuration conf = this.getConf();
                Job job = Job.getInstance(conf, "page count");
                job.setJarByClass(WikipediaPopular.class);

                job.setInputFormatClass(TextInputFormat.class);

                job.setMapperClass(TokenizerMapper.class);
//                job.setCombinerClass(LongSumReducer.class);
                job.setReducerClass(IntSumReducer.class);

                job.setOutputKeyClass(Text.class);
                job.setOutputValueClass(IntWritable.class);
                job.setOutputFormatClass(TextOutputFormat.class);
                TextInputFormat.addInputPath(job, new Path(args[0]));
                TextOutputFormat.setOutputPath(job, new Path(args[1]));

                return job.waitForCompletion(true) ? 0 : 1;
        }
}


import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai1 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text ratingValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 4) {
                return;
            }

            try {
                String movieId = parts[1].trim();
                double rating = Double.parseDouble(parts[2].trim());
                movieIdKey.set(movieId);
                ratingValue.set(String.format("%.2f", rating));
                context.write(movieIdKey, new Text("Rate: " + rating));

            } catch (NumberFormatException e) {

            }
        }

    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieNameValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 3) {
                return;
            }

            try {
                String movieId = parts[0].trim();
                String movieIdName = parts[1].trim();
                movieIdKey.set(movieId);
                movieNameValue.set(movieIdName);
                context.write(movieIdKey, new Text("Name: " + movieNameValue));

            } catch (NumberFormatException e) {

            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
            private Text outputKey = new Text();
            private Text outputValue = new Text();

            public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                double sum = 0.0;
                int count = 0;
                String movieName = "Unknown";

                for (Text val : values) {
                    String strVal = val.toString();
                    if (strVal.startsWith("Rate: ")) {
                        try {
                            double rating = Double.parseDouble(strVal.replace("Rate: ", ""));
                            sum += rating;
                            count++;
                        } catch (NumberFormatException e) {
                        }
                    } 
                    else if (strVal.startsWith("Name: ")) {
                        movieName = strVal.replace("Name: ", "");
                    }
                }

            if (count > 0) {
                double average = sum / count;
                outputKey.set(movieName); 
                outputValue.set(String.format("    Average rating: %.1f (Total ratings: %d)", average, count));
                context.write(outputKey, outputValue);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "bai1");
                job.setJarByClass(bai1.class);

        // job.setMapperClass(RatingReducer.class);
        // job.setMapperClass(MovieMapper.class);
        job.setReducerClass(RatingReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(
            job, 
            new Path(args[0]), 
            org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, 
            RatingMapper.class
        );

        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(
            job, 
            new Path(args[1]), 
            org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class, 
            MovieMapper.class
        );

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
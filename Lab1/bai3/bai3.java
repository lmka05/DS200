import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai3 {

    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text R_Key = new Text(); // Key: U ID
        private Text R_Value = new Text();

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
                String Key = parts[0].trim();
                String Val = "R|" + parts[1] + ":" + parts[2];
                R_Key.set(Key);
                R_Value.set(Val);
                context.write(R_Key, R_Value);

            } catch (NumberFormatException e) {

            }
        }
    }

    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        private Text U_Key = new Text(); // U ID [0]
        private Text U_Value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length < 5) {
                return;
            }

            try {
                String Key = parts[0].trim();
                String Val = "U|" + parts[1].trim();
                U_Key.set(Key);
                U_Value.set(Val);
                context.write(U_Key, U_Value);

            } catch (NumberFormatException e) {

            }
        }
    }

    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String gender = "";
            ArrayList<String> ratingBuffer = new ArrayList<>();

            for (Text val : values) {
                String str = val.toString();

                if (str.startsWith("U|")) {
                    gender = str.substring(2).trim();
                } else if (str.startsWith("R|")) {
                    ratingBuffer.add(str.substring(2).trim());
                }
            }

            if (!gender.isEmpty()) {
                for (String ratingInfo : ratingBuffer) {
                    String[] parts = ratingInfo.split(":");
                    
                    if (parts.length == 2) {
                        String movieId = parts[0];
                        String score = parts[1];
                        outputKey.set(movieId);
                        outputValue.set(gender + ":" + score);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }    

    public static class GenderMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\t");

            if (parts.length == 2) {
                outputKey.set(parts[0]); 
                outputValue.set("G|" + parts[1]); // G|Gender:scỏe
                context.write(outputKey, outputValue);
            } 
        }
    }

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length >= 2) {
                outputKey.set(parts[0]); // M ID
                outputValue.set("T|" + parts[1]);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static class GenderReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            double maleSum = 0;
            double femaleSum = 0;
            int maleCount = 0;
            int femaleCount = 0;

            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("T|")) {
                    movieTitle = str.substring(2); 
                } else if (str.startsWith("G|")) {
                    String data = str.substring(2); 
                    String[] parts = data.split(":");
                    String gender = parts[0];
                    double score = Double.parseDouble(parts[1]);

                    if (gender.equalsIgnoreCase("M")) {
                        maleSum += score; maleCount++;
                    } else if (gender.equalsIgnoreCase("F")) {
                        femaleSum += score; femaleCount++;
                    }
                }
            }

            if (maleCount > 0 || femaleCount > 0) {
                double maleAvg;
                double femaleAvg;

                if (maleCount > 0) {
                    maleAvg = maleSum / maleCount;
                } else {
                    maleAvg = 0.0;
                }

                if (femaleCount > 0) {
                    femaleAvg = femaleSum / femaleCount;
                } else {
                    femaleAvg = 0.0;
                }

                String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);
                outputKey.set(movieTitle);
                outputValue.set(result);
                context.write(outputKey, outputValue);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path intermediatePath = new Path("intermediate_output_bai3");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(intermediatePath)) {
            fs.delete(intermediatePath, true);
        }

        Job job1 = Job.getInstance(conf, "Job 1");
        job1.setJarByClass(bai3.class);

        job1.setReducerClass(RatingReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(
            job1, 
            new Path(args[0]), 
            TextInputFormat.class, 
            RatingMapper.class
        );

        MultipleInputs.addInputPath(
            job1, 
            new Path(args[1]), 
            TextInputFormat.class, 
            UserMapper.class
        );
        FileOutputFormat.setOutputPath(job1, intermediatePath);

        if (job1.waitForCompletion(true)) {
            Job job2 = Job.getInstance(conf, "Job 2");
            job2.setJarByClass(bai3.class);

            job2.setReducerClass(GenderReducer.class); 
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(
                job2, 
                intermediatePath, 
                TextInputFormat.class, 
                GenderMapper.class
            );

            MultipleInputs.addInputPath(
                job2, 
                new Path(args[2]), 
                TextInputFormat.class, 
                MovieMapper.class
            );
            FileOutputFormat.setOutputPath(job2, new Path(args[3]));

            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }


} 

















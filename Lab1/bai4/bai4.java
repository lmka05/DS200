import java.io.IOException;
import java.util.ArrayList; 
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem; 
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class bai4 {

    // key : Userid, value : ["R| 2352 : 4.0", "R|2542 : 4.5"], neu 1 user danh gia nhieu phim thi se co nhieu dong
    public static class RatingMapper extends Mapper<Object, Text, Text, Text> {
        private Text R_Key = new Text(); // user id
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
                context.write(R_Key, R_Value); // U M R

            } catch (NumberFormatException e) {

            }
        }
    }

    // key : Userid, value : U| ageGroup
    public static class UserMapper extends Mapper<Object, Text, Text, Text> {
        private Text U_Key = new Text();
        private Text U_Value = new Text(); // Age [2]

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
                int age = Integer.parseInt(parts[2].trim());      
                String ageGroup = "";

                if (age <= 18) {
                ageGroup = "0-18";
                } else if (age <= 35) {
                    ageGroup = "18-35";
                } else if (age <= 50) {
                    ageGroup = "35-50";
                } else {
                    ageGroup = "50+";
                }     

                U_Key.set(Key);
                U_Value.set("U|" + ageGroup);
                context.write(U_Key, U_Value);     
            }catch (NumberFormatException e) {

            }
            
        }
    }
    
    /*
    Input: 642 -> [U|18-35, R|2589:3.0, R|2535:3.5]

    Output: 2589 -> 18-35:3.0 
    luu y : moi dong co the trung lap movieid
    */
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String ageGroup = "";
            ArrayList<String> ratingBuffer = new ArrayList<>();

            for (Text val : values) {
                String str = val.toString();
                
                if (str.startsWith("U|")) {
                    ageGroup = str.substring(2).trim();
                } else if (str.startsWith("R|")) {
                    ratingBuffer.add(str.substring(2).trim());
                }
            }

            if (!ageGroup.isEmpty()) {
                for (String ratingData : ratingBuffer) {
                    String[] parts = ratingData.split(":");
                    
                    if (parts.length == 2) {
                        String movieId = parts[0];
                        String score = parts[1];
                        outputKey.set(movieId);
                        outputValue.set(ageGroup + ":" + score);
                        context.write(outputKey, outputValue);
                    }
                }
            }
        }
    }

    // output :2589 -> A |18-35:3.0
    public static class AgeGroupMapper extends Mapper<Object, Text, Text, Text> {
        private Text A_Key = new Text();
        private Text A_Value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split("\t");
            
            if (parts.length == 2) {
                String movieId = parts[0].trim();
                String ageAndScore = parts[1].trim(); // A:R
                A_Key.set(movieId);
                A_Value.set("A|" + ageAndScore); 
                context.write(A_Key, A_Value);
            }
        }
    }

    // Mapper xu ly movie.txt -> key: movieID, value : T| movietitle
    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text M_Key = new Text();
        private Text M_Value = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(",");

            if (parts.length >= 2) {
                M_Key.set(parts[0].trim()); 
                String movieTitle = parts[1].trim(); 
                M_Value.set("T|" + movieTitle);
                context.write(M_Key, M_Value);
            }
        }
    }

    /*
    Input : key : movieID, values : [T|movietitle, A| 18-35:5.0,...]
    */
    public static class AgeGroupReducer extends Reducer<Text, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String movieTitle = "Unknown";
            double sum0_18 = 0, sum18_35 = 0, sum35_50 = 0, sum50_plus = 0;
            int count0_18 = 0, count18_35 = 0, count35_50 = 0, count50_plus = 0;

            for (Text val : values) {
                String str = val.toString();
                
                if (str.startsWith("T|")) {
                    movieTitle = str.substring(2).trim();
                } else if (str.startsWith("A|")) {
                    String[] parts = str.substring(2).split(":");
                    if (parts.length == 2) {
                        String group = parts[0];
                        double score = Double.parseDouble(parts[1]);

                        if (group.equals("0-18")) {
                            sum0_18 += score; count0_18++;
                        } else if (group.equals("18-35")) {
                            sum18_35 += score; count18_35++;
                        } else if (group.equals("35-50")) {
                            sum35_50 += score; count35_50++;
                        } else if (group.equals("50+")) {
                            sum50_plus += score; count50_plus++;
                        }
                    }
                }
            }

            String res0_18, res18_35, res35_50, res50_plus;

            if (count0_18 > 0) { 
                res0_18 = String.format("%.2f", sum0_18 / count0_18); 
            } else { 
                    res0_18 = "NA"; 
            }

            if (count18_35 > 0) { 
                res18_35 = String.format("%.2f", sum18_35 / count18_35); 
            } else { 
                res18_35 = "NA"; 
            }

            if (count35_50 > 0) { 
                res35_50 = String.format("%.2f", sum35_50 / count35_50); 
            } else { 
                res35_50 = "NA";
            }

            if (count50_plus > 0) { 
                res50_plus = String.format("%.2f", sum50_plus / count50_plus); 
            } else { 
                res50_plus = "NA"; 
            }

            String finalResult = String.format("0-18: %s, 18-35: %s, 35-50: %s, 50+: %s", res0_18, res18_35, res35_50, res50_plus);

            outputKey.set(movieTitle);
            outputValue.set(finalResult);
            context.write(outputKey, outputValue);
        }
    }
    
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path intermediatePath = new Path("intermediate_output_bai4");
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(intermediatePath)) {
            fs.delete(intermediatePath, true);
        }
        if (fs.exists(new Path(args[3]))) {
            fs.delete(new Path(args[3]), true);
        }

        Job job1 = Job.getInstance(conf, "Job 1");
        job1.setJarByClass(bai4.class);

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
            job2.setJarByClass(bai4.class);

            job2.setReducerClass(AgeGroupReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            MultipleInputs.addInputPath(
                job2, 
                intermediatePath, 
                TextInputFormat.class, 
                AgeGroupMapper.class
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

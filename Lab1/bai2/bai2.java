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

public class bai2 {

    public static class MovieMapper extends Mapper<Object, Text, Text, Text> {
        private Text movieIdKey = new Text();
        private Text movieGenresValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim(); //  lay noi dung cua dong
            if (line.isEmpty()) {
                return;
            }

            String[] parts = line.split(","); // tao 1 list string bang cach lay tung phan tu ngan cach bang dau 

            if (parts.length < 3) {
                return;
            }

            try {
                String movieId = parts[0].trim();
                String movieIdGenres = parts[2].trim();
                movieIdKey.set(movieId);
                movieGenresValue.set(movieIdGenres);
                context.write(movieIdKey, new Text("Genres: " + movieGenresValue));

            } catch (NumberFormatException e) {

            }
        }
    }

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
    /* 
     class nay se xu ly qua tung movieID
     VD : {key : 1080, values : [Genres : Sci-fi | Comedy, Rates : 4, Rates : 5]} 
       => Action      13.5,3
          Adventure   13.5,3
          Sci-Fi      13.5,3
    */
    public static class RatingReducer extends Reducer<Text, Text, Text, Text> {
        private Text movieGenresKey = new Text();
        private Text TotalandCountValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sum = 0.0;
            int count = 0;
            String movieGenres = "Unknown";
            // key : 1080,   values : list cac rate va genres. VD : [Genres: sci-fi | comedy, Rates: 5, Rates: 4]
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
                else if (strVal.startsWith("Genres: ")) {
                    movieGenres = strVal.replace("Genres: ", "");
                }
            }

            if (count > 0) {
                String[] genreList = movieGenres.split("\\|"); // ["Action", "Adventure", "Sci-Fi"]
                TotalandCountValue.set(sum + "," + count);
                for (String g : genreList) {
                    movieGenresKey.set(g.trim());
                    context.write(movieGenresKey, TotalandCountValue);
                }
            }
        }
    }

    /* 
    Du lieu sau khi di qua class nay se bien doi tu
        Action      13.5,3
        Adventure   13.5,3   

        => Action 
            13.5,3
            20.0,5
            8.0,2

           Adventure:
            10.0,2
            6.0,1

    */
    public static class GenresMapper extends Mapper<Object, Text, Text, Text> {
        private Text outputKey = new Text();
        private Text outputValue = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            if (parts.length == 2) {
                outputKey.set(parts[0].trim());   
                outputValue.set(parts[1].trim()); 
                context.write(outputKey, outputValue);
                
            }
        }
    }

    public static class GenresReducer extends Reducer<Text, Text, Text, Text> {
        private Text resultValue = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double totalSum = 0.0;
            long totalCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(",");
                if (parts.length == 2) {
                    totalSum += Double.parseDouble(parts[0]); 
                    totalCount += Long.parseLong(parts[1]);   
                }
            }

            if (totalCount > 0) {
                double average = totalSum / totalCount;
                String finalString = String.format("Avg: %.2f, Count: %d", average, totalCount);
                resultValue.set(finalString);
                context.write(key, resultValue);
            }
        }
    }

    /*
    Job 1:
    movies + ratings
    → join theo movieId
    → (genre, sum,count)   → intermediate_output_bai2

    Job 2:
    (intermediate)
    → group theo genre
    → avg rating theo genre → output_bai2
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path intermediatePath = new Path("intermediate_output_bai2"); // duong dan output trung gian

        //tao job 1     
        Job job1 = Job.getInstance(conf, "bai2_job1");
        job1.setJarByClass(bai2.class);

        job1.setReducerClass(RatingReducer.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        // Input path cho MovieMapper
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(
            job1,
            new Path(args[0]),
            org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
            MovieMapper.class
        );

        //Input path cho Rating Mapper
        org.apache.hadoop.mapreduce.lib.input.MultipleInputs.addInputPath(
            job1,
            new Path(args[1]),
            org.apache.hadoop.mapreduce.lib.input.TextInputFormat.class,
            RatingMapper.class
        );

        //Output cho job1
        org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job1, intermediatePath);

        if (job1.waitForCompletion(true)) {
            
            //tao job 2
            Job job2 = Job.getInstance(conf, "bai2_job2");
            job2.setJarByClass(bai2.class);

            job2.setMapperClass(GenresMapper.class);
            job2.setReducerClass(GenresReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);

            //Input va Ouput path cho job 2
            org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job2, intermediatePath);
            org.apache.hadoop.mapreduce.lib.output.FileOutputFormat.setOutputPath(job2, new Path(args[2]));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        }
    }

}
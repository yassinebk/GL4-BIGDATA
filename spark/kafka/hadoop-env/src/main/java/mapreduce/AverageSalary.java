package mapreduce;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageSalary {

    public static class SalaryMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {
        private final static DoubleWritable one = new DoubleWritable(1);
        private Text salaryKey = new Text("salary");

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] fields = line.split(",");

            // Assuming the salary fields are max_salary, med_salary, and min_salary
            if (fields.length > 5) {
                double maxSalary = Double.parseDouble(fields[4]);
                double medSalary = Double.parseDouble(fields[5]);
                double minSalary = Double.parseDouble(fields[6]);

                // Calculate the average salary for each job posting
                double averageSalary = (maxSalary + medSalary + minSalary) / 3;
                context.write(salaryKey, new DoubleWritable(averageSalary));
            }
        }
    }

    public static class SalaryReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable result = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            double sum = 0;
            int count = 0;
            for (DoubleWritable val : values) {
                sum += val.get();
                count++;
            }
            result.set(sum / count);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Average Salary");
        job.setJarByClass(AverageSalary.class);
        job.setMapperClass(SalaryMapper.class);
        job.setCombinerClass(SalaryReducer.class);
        job.setReducerClass(SalaryReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
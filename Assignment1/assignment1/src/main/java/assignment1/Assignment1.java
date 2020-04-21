/*
Student Name: lijiachen
Student ID: z5184142
*/

package assignment1;

import java.io.IOException;

import java.util.*;


import java.io.DataInput;
import java.io.DataOutput;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.Writable;

public class Assignment1 {
    /*
    Array Class:
    Class to store amount and file_name of the key.
    Having getting and setting function to set or get the value of the parameters.
    */
    public static class Array implements Writable{
        private Integer amount;
        private String file_name;

        public Array(){

        }
        public Array(Integer amount, String file_name) {
            this.amount = amount;
            this.file_name = file_name;
        }

        public Integer getAmount() {
            return amount;
        }

        public void setAmount(Integer amount) {
            this.amount = amount;
        }

        public String getFile_name() {
            return file_name;
        }

        public void setFile_name(String file_name) {
            this.file_name = file_name;
        }

        public void write(DataOutput out) throws IOException {
            out.writeInt(amount);
            out.writeUTF(file_name);
        }

        public void readFields(DataInput in) throws IOException {
            this.amount = in.readInt();
            this.file_name = in.readUTF();
        }
    }

    /*
    TokenizerMapper Class:
    Mapper Class
    Split the input text and put the value of text in the queue.
    Once the queue's size is not smaller than ngram, set amount as 1 and file_name to the array class.
    After that invoke context.write to write to the output of the mapper.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, Array> {

        public static Integer ngram;
        //Set the ngram as the capacity of the queue.
        public void setNgram(Integer ngram) {
            this.ngram = ngram;
        }

        private Array array = new Array();
        private Text word = new Text();
        Queue<String> queue = new LinkedList<String>();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                if (queue.size() < ngram) {
                    queue.offer(itr.nextToken());
                } else {
                    word.set(queueToString(queue));
                    array.setAmount(1);
                    array.setFile_name(((FileSplit)context.getInputSplit()).getPath().getName());
                    context.write(word, array);
                    queue.poll();
                    queue.offer(itr.nextToken());
                }

            }
            word.set(queueToString(queue));
            array.setAmount(1);
            array.setFile_name(((FileSplit)context.getInputSplit()).getPath().getName());
            context.write(word, array);
        }

        /*
        Process the elements in queue to a string.
         */
        public String queueToString(Queue<String> queue){
            String x = "";
            for(String xx: queue){
                x += xx+" ";
            }
            return  x.substring(0,x.length()-1);
        }
    }


    /*
    Sum the value of the same key in all the files and splice the file name together.
    Write to output.
     */
    public static class OutputReducer extends Reducer<Text, Array, Text, Text> {
        private Text result = new Text();
        public static Integer count;
        public void setCount(Integer count) {
            this.count = count;
        }
        public void reduce(Text key, Iterable<Array> values, Context context) throws IOException, InterruptedException {
            Integer sum = 0;
            List<String> file_name = new ArrayList<String>();
            String file_name_1 = "";
            for (Array val : values) {
                sum += val.getAmount();
                file_name.add(val.getFile_name());//Using an List to get all the filename of the key.
            }
            if (sum >= count) {
                Set<String> set = new HashSet<String>(file_name);//Using set to eliminate the repeated filename.
                List<String> newList = new ArrayList<String>(set);
                for(int i=0;i<newList.size();i++){
                    file_name_1 += (String)newList.get(i)+" ";
                }
                result.set(sum.toString() +"  "+file_name_1);
                context.write(key, result);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        TokenizerMapper tokenizerMapper = new TokenizerMapper();
        tokenizerMapper.setNgram(Integer.parseInt(args[0]));
        job.setJarByClass(Assignment1.class);
        job.setMapperClass(tokenizerMapper.getClass());
        //job.setCombinerClass(MultiFileSumReducer.class);
        OutputReducer outputReducer = new OutputReducer();
        outputReducer.setCount(Integer.parseInt(args[1]));
        job.setReducerClass(outputReducer.getClass());
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Array.class);
        FileInputFormat.addInputPath(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

        System.out.println(tokenizerMapper.getClass());
    }
}
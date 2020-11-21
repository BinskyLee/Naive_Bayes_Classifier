import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class Main {
    public static void main(String[] args) throws Exception{
        // 处理传入配置文件【HDFS目录】，【训练集绝对/相对路径】，【测试集绝对/相对路径】，【输出目录】
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if(otherArgs.length != 4)
        {
            System.err.println("Usage: NaiveBayesMain <hdfs_path> <train> <test> <out>");
            System.exit(2);
        }
        setupPath(otherArgs);
        FileSystem fs = FileSystem.get(conf);

//        conf.set("train", PathUtils.TRAIN_PATH);
//        conf.set("test", PathUtils.TEST_PATH);
//        conf.set("output", PathUtils.OUTPUT_PATH);
//        conf.set("train_result", PathUtils.TRAIN_RESULT);
//        conf.set("evaluation_origin", PathUtils.EVALUATION_ORIGIN);
//        conf.set("evaluation_classified", PathUtils.EVALUATION_CLASSIFIED);
//        conf.set("train", otherArgs[0] + "/" +otherArgs[1]);
//        conf.set("test", otherArgs[0] + "/" +otherArgs[2]);
//        conf.set("output", otherArgs[0] + "/" +otherArgs[3]);
//        conf.set("train_result", otherArgs[0] + "/" +otherArgs[1] + ".train");
        put2HDFS(otherArgs[1], PathUtils.TRAIN_PATH, conf);
        put2HDFS(otherArgs[2], PathUtils.TEST_PATH, conf);

//        put2HDFS(otherArgs[1], otherArgs[0] + "/" + otherArgs[1], conf);
//        put2HDFS(otherArgs[2], otherArgs[0] + "/" + otherArgs[2], conf);

//        path_train = new Path(PathUtils.TEST_PATH); // 模型1输入
//        path_train_result = new Path(PathUtils)
//        path_train = new Path(otherArgs[0] + "/" + otherArgs[1]);// 模型1输入
//        path_temp = new Path(otherArgs[0] + "/" + otherArgs[1] + ".train"); // 缓存模型1和模型2之间的数据
//        path_test = new Path(otherArgs[0] + "/" +otherArgs[2]); //模型2输入
//        path_out = new Path(otherArgs[0] + "/" + otherArgs[3]); //模型2输出
//        path_evaluation1 = new Path(otherArgs[0] + "/" + otherArgs[1] + ".evaluation1");
//        path_evaluation2 = new Path(otherArgs[0] + "/" + otherArgs[1] + ".evaluation2");

        //模型1：处理训练集，Mapper对应训练集一行文本
        {
            // 配置Map-Reduce对应的类（方法）
            Job job_train = Job.getInstance(conf);
            job_train.setJobName("Naive Bayes Training");
//            job_train.setJarByClass(Main.class); // 处理类
            job_train.setJarByClass(NaiveBayes.class);
            job_train.setMapperClass(WordCount.WordCountMapper.class);
            job_train.setCombinerClass(WordCount.WordCountReduce.class);
            job_train.setReducerClass(WordCount.WordCountReduce.class);
            job_train.setOutputKeyClass(Text.class);
            job_train.setOutputValueClass(IntWritable.class);
            job_train.setOutputFormatClass(SequenceFileOutputFormat.class);

            // 配置Map-Reduce对应的输入输出路径
            FileInputFormat.setInputPaths(job_train, new Path(PathUtils.TRAIN_PATH));
            if(fs.exists(new Path(PathUtils.TRAIN_RESULT))) // 存在路径则删除，否则会报错
                fs.delete(new Path(PathUtils.TRAIN_RESULT), true);
            FileOutputFormat.setOutputPath(job_train, new Path(PathUtils.TRAIN_RESULT));
            if(!job_train.waitForCompletion(true))
                System.exit(1);
        }
        //模型2：处理测试集，Mapper对应测试集一行文本
        {
            // 配置Map-Reduce对应的类（方法）
            Job job_test = Job.getInstance(conf);
            job_test.setJobName("Naive Bayes Testing");
            job_test.setJarByClass(NaiveBayes.class);            // setUp： 读取模型1输出
            job_test.setMapperClass(NaiveBayes.NBMapper.class);   // Mapper步骤，使用NB对于输入句子预测
            job_test.setCombinerClass(NaiveBayes.NBReducer.class); // Combiner，Reduce步骤：利用输入集合的Id进行排序输出
            job_test.setReducerClass(NaiveBayes.NBReducer.class);
            job_test.setOutputKeyClass(Text.class);
            job_test.setOutputValueClass(Text.class);

            // 配置Map-Reduce对应的输入输出路径
            FileInputFormat.setInputPaths(job_test, new Path(PathUtils.TEST_PATH));
            if(fs.exists(new Path(PathUtils.OUTPUT_PATH)))
                fs.delete(new Path(PathUtils.OUTPUT_PATH), true);
            FileOutputFormat.setOutputPath(job_test, new Path(PathUtils.OUTPUT_PATH));
            if(!job_test.waitForCompletion(true))
                System.exit(1);
            fs.delete(new Path(PathUtils.TRAIN_RESULT), true);
        }

        // 评估模型
        {
            // 配置Map-Reduce对应的类（方法）
            Job job_evaluation1 = Job.getInstance(conf);
            job_evaluation1.setJobName("Naive Bayes Evaluation1");
            job_evaluation1.setJarByClass(NaiveBayes.class);
            job_evaluation1.setMapperClass(Evaluation.Map.class);
            job_evaluation1.setCombinerClass(Evaluation.Reduce.class);
            job_evaluation1.setReducerClass(Evaluation.Reduce.class);
            job_evaluation1.setOutputKeyClass(Text.class);
            job_evaluation1.setOutputValueClass(Text.class);
            job_evaluation1.setOutputFormatClass(SequenceFileOutputFormat.class);
            // 配置Map-Reduce对应的输入输出路径
            FileInputFormat.setInputPaths(job_evaluation1, new Path(PathUtils.TEST_PATH));
            if(fs.exists(new Path(PathUtils.EVALUATION_ORIGIN)))
                fs.delete(new Path(PathUtils.EVALUATION_ORIGIN), true);
            FileOutputFormat.setOutputPath(job_evaluation1, new Path(PathUtils.EVALUATION_ORIGIN));
            if(!job_evaluation1.waitForCompletion(true))
                System.exit(1);
        }

        {
            // 配置Map-Reduce对应的类（方法）
            Job job_evaluation2 = Job.getInstance(conf);
            job_evaluation2.setJobName("Naive Bayes Evaluation2");
            job_evaluation2.setJarByClass(NaiveBayes.class);
            job_evaluation2.setMapperClass(Evaluation.Map.class);
            job_evaluation2.setCombinerClass(Evaluation.Reduce.class);
            job_evaluation2.setReducerClass(Evaluation.Reduce.class);
            job_evaluation2.setOutputKeyClass(Text.class);
            job_evaluation2.setOutputValueClass(Text.class);
            job_evaluation2.setOutputFormatClass(SequenceFileOutputFormat.class);
            // 配置Map-Reduce对应的输入输出路径
            FileInputFormat.setInputPaths(job_evaluation2, new Path(PathUtils.OUTPUT_PATH + PathUtils.OUTPUT_SUB));
//            FileInputFormat.setInputPaths(job_evaluation2, new Path(otherArgs[3] + "/part-r-00000"));
            if(fs.exists(new Path(PathUtils.EVALUATION_CLASSIFIED)))
                fs.delete(new Path(PathUtils.EVALUATION_CLASSIFIED), true);
            FileOutputFormat.setOutputPath(job_evaluation2, new Path(PathUtils.EVALUATION_CLASSIFIED));
            if(!job_evaluation2.waitForCompletion(true))
                System.exit(1);
        }
        Evaluation.GetEvaluation(conf, PathUtils.EVALUATION_CLASSIFIED + PathUtils.OUTPUT_SUB, PathUtils.EVALUATION_ORIGIN + PathUtils.OUTPUT_SUB);
//        Evaluation.GetEvaluation(conf, otherArgs[3] + "/evaluation2" + "/part-r-00000", otherArgs[3] + "/evaluation1" + "/part-r-00000");
        getFromHDFS(PathUtils.OUTPUT_PATH, ".", conf);

        fs.close();
        System.exit(0);
    }

    // 模型从HDFS存储文件
    public static void put2HDFS(String src, String dst, Configuration conf) throws Exception {
        Path dstPath = new Path(dst);
        FileSystem fs = dstPath.getFileSystem(conf);

        fs.copyFromLocalFile(false, true, new Path(src), dstPath);
    }
    // 模型从HDFS读取文件
    public static void getFromHDFS(String src, String dst, Configuration conf) throws Exception {
        FileSystem fs = new Path(dst).getFileSystem(conf);
        String[] arrayOfString = src.split("/");
        Path dstPath = new Path(arrayOfString[(arrayOfString.length - 1)]);
        fs.delete(dstPath, true);
        fs.copyToLocalFile(false, new Path(src), dstPath);
    }

    private static void setupPath(String[] args){
        PathUtils.HDFS_PATH = args[0];
        PathUtils.TRAIN_PATH = args[0] + "/" + args[1];
        PathUtils.TEST_PATH = args[0] + "/" + args[2];
        PathUtils.OUTPUT_PATH = args[0] + "/" +args[3];
        PathUtils.TRAIN_RESULT = args[0] + "/" + "train";
        PathUtils.EVALUATION_ORIGIN = args[0] + "/" + "evaluation/origin";
        PathUtils.EVALUATION_CLASSIFIED = args[0] + "/" + "evaluation/classified";
    }
}

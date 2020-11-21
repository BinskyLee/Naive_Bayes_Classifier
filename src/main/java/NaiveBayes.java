import org.apache.commons.lang.CharUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.Iterator;

public class NaiveBayes {

    public static  class NBMapper extends Mapper<LongWritable, Text, Text, Text>{
        final double ALPHA = 1.0E-7D;
//        public WordCountReader train;
        private CountStatistic train;

        // setup会在每个Mapper之前执行，初始化数据
        public void setup(Context context){
            try{
                Configuration conf = context.getConfiguration();
//                Configuration localConfiguration = context.getConfiguration();
//                train = new WordCountReader();
                train = new CountStatistic();
                train.CalculateData(conf, PathUtils.TRAIN_RESULT + PathUtils.OUTPUT_SUB);
//                train.CalculateData(conf, conf.get("train_result") + PathUtils.OUTPUT_SUB);
//                train.getData(localConfiguration.get("train_result"), localConfiguration);
            }catch (Exception localException)   {
                localException.printStackTrace();
                System.exit(1);
            }
        }

        public void map(LongWritable key, Text value, Context context){
            try{
                String[] valueArray = value.toString().split("\t");
                String id = valueArray[0];
                String review = valueArray[1];
                // 计算每个标签的概率
                StringReader reader = new StringReader(review);
                double p0 = Math.log(ALPHA + train.getEachLabelNumInTrainData().get("0") * 1.0D / train.getEachLabelNumInTrainData().size());
                double p1 = Math.log(ALPHA + train.getEachLabelNumInTrainData().get("1") * 1.0D / train.getEachLabelNumInTrainData().size());
                // 分词
                IKSegmenter iks = new IKSegmenter(reader, true);
                Lexeme lexeme = null;
                while ((lexeme = iks.next()) != null) {
                    String word = lexeme.getLexemeText();
                    // 过滤掉以字母和数字开头的词
                    if(CharUtils.isAsciiAlphanumeric(word.charAt(0))){
                        continue;
                    }
                    p0 += Math.log(train.getEachWordNumInLabel().getOrDefault("0\t" + word, 0) + ALPHA) - Math.log(train.getEachLabelNumInTrainData().get("0") + train.getEachWordNumInTrainData().size() * ALPHA);
                    p1 += Math.log(train.getEachWordNumInLabel().getOrDefault("1\t" + word, 0) + ALPHA) - Math.log(train.getEachLabelNumInTrainData().get("1") + train.getEachWordNumInTrainData().size() * ALPHA);
                }
                // 选取最高的作为预测的结果
                String label = ( p0 >= p1 ? "0" : "1");
                // 输出、传递参数
                context.write(new Text(id + "\t" + review), new Text(label));
            } catch (Exception localException) {
                localException.printStackTrace();
            }
        }
    }
    public static class NBReducer extends Reducer<Text, Text, Text, Text>{

        public void reduce(Text label, Iterable<Text> reviews, Context context)throws IOException, InterruptedException{
            Iterator iterator = reviews.iterator();
            while(iterator.hasNext()){
                Text review = (Text)iterator.next();
                context.write(review, label);
            }
        }
    }
}

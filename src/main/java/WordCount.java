import org.apache.commons.lang.CharUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;

public class WordCount {
    // 统计词频
    // 输出格式 <label + "\t" + word, count>
    public static class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
        final IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArray = value.toString().split("\t");
            String review = valueArray[1];
            String label = valueArray[2];
            StringReader reader = new StringReader(review);
            // 分词
            IKSegmenter iks = new IKSegmenter(reader, true);
            Lexeme lexeme = null;
            while ((lexeme = iks.next()) != null) {
                String out = lexeme.getLexemeText();
                if(CharUtils.isAsciiAlphanumeric(out.charAt(0))){
                    continue;
                }
                word.set(label + "\t" + out);
                context.write(word, one);
            }
        }
    }

    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
        public void  reduce(Text word, Iterable<IntWritable> cnt, Context context) throws IOException, InterruptedException{
            int val = 0;
            for(IntWritable v : cnt){
                val += v.get();
            }
            context.write(word, new IntWritable(val));
        }
    }
}

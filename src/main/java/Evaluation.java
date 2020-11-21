import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;

public class Evaluation {
    /**
     * 将<id + "\t" + review, label>转换为<label, id>的形式
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] valueArray = value.toString().split("\t");
            String id = valueArray[0];
            String label = valueArray[2];
            context.write(new Text(label), new Text(id));
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // 生成分类列表
            String idList = new String();
            for(Text value: values){
                idList += value.toString() + ",";
            }
            context.write(key, new Text(idList));
        }
    }

    //Precision精度:P = TP/(TP+FP)
    //Recall精度:   R = TP/(TP+FN)
    //P和R的调和平均:F1 = 2PR/(P+R)
    public static void GetEvaluation(Configuration conf, String ClassifiedPath, String OriginalPath) throws IOException{
        Path path1 = new Path(ClassifiedPath);
        SequenceFile.Reader reader1 = null;

        Path path2 = new Path(OriginalPath);
        SequenceFile.Reader reader2 = null;
        try{
            SequenceFile.Reader.Option pathOption1 = SequenceFile.Reader.file(path1);
            reader1 = new SequenceFile.Reader(conf, pathOption1);

            Text key1 = (Text)ReflectionUtils.newInstance(reader1.getKeyClass(), conf);
            Text value1 = (Text)ReflectionUtils.newInstance(reader1.getValueClass(), conf);

            SequenceFile.Reader.Option pathOption2 = SequenceFile.Reader.file(path2);
            reader2 = new SequenceFile.Reader(conf, pathOption2);

            Text key2 = (Text)ReflectionUtils.newInstance(reader2.getKeyClass(), conf);
            Text value2 = (Text)ReflectionUtils.newInstance(reader2.getValueClass(), conf);

            reader1.next(key1, value1);
            reader2.next(key2, value2);

            String[] values1 = value1.toString().split(",");
            String[] values2 = value2.toString().split(",");

            int TP = 0;
            for(String str1:values1){
                for(String str2:values2){
                    if(str1.equals(str2)){
                        TP++;
                        break;
                    }
                }
            }
            double precision = TP * 1.0 / values1.length;
            double recall = TP * 1.0 / values2.length;
            double f1 = 2 * precision * recall / (precision + recall);
            System.out.println("Precision: " + precision);
            System.out.println("Recall: " + recall);
            System.out.println("F1: " + f1);
        }finally{
            if(reader1 != null){
                reader1.close();
            }
            if(reader2 != null){
                reader2.close();
            }
        }
    }
}

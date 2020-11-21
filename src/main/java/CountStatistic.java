import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.HashMap;

public class CountStatistic {
    private HashMap<String, Integer> eachWordNumInLabel; // 每个词在每一个标签下的数目
    private HashMap<String, Integer> eachWordNumInTrainData; // 每个词在训练集中的总数目
    private HashMap<String, Integer> eachLabelNumInTrainData; // 每个标签在训练集中的总数目

    public CountStatistic(){
        eachWordNumInLabel = new HashMap<>();
        eachWordNumInTrainData = new HashMap<>();
        eachLabelNumInTrainData = new HashMap<>();
    }

    public HashMap<String, Integer> getEachWordNumInLabel() {
        return eachWordNumInLabel;
    }

    public HashMap<String, Integer> getEachWordNumInTrainData() {
        return eachWordNumInTrainData;
    }

    public HashMap<String, Integer> getEachLabelNumInTrainData() {
        return eachLabelNumInTrainData;
    }

    public void CalculateData(Configuration conf, String pathString) throws IOException {
        Path path = new Path(pathString);
        SequenceFile.Reader reader = null;
        try {
            SequenceFile.Reader.Option pathOption = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(conf, pathOption);

            Text key = (Text) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            IntWritable value = (IntWritable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
            while(reader.next(key, value)){
                eachWordNumInLabel.put(key.toString(), value.get());
                String[] keys = key.toString().split("\t");
                String label = keys[0];
                String word = keys[1];
                eachLabelNumInTrainData.put(label, eachLabelNumInTrainData.getOrDefault(label, 0) + value.get());
                eachWordNumInTrainData.put(word, eachWordNumInTrainData.getOrDefault(word, 0) + value.get());
            }
        }finally {
            if(reader != null){
                reader.close();
            }
        }





    }

}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;

public class WordCountReader {
    public HashMap<String, Integer> freq;
    public HashMap<String, Integer> labelF;
    public HashMap<String, Integer> wordF;

    public WordCountReader(){
        freq = new HashMap<>(); // save N(word, label)
        labelF = new HashMap<>(); // save N(label)
        wordF = new HashMap<>(); // save N(word)
    }

    public void getData(String paramString, Configuration paramConfiguration) throws IOException {
        Path localPath1 = new Path(paramString);

        FileSystem localFileSystem = localPath1.getFileSystem(paramConfiguration);
        FileStatus[] arrayOfFileStatus = localFileSystem.listStatus(localPath1);
        Integer localInteger1 = 0;

        for (int i = 0; i < arrayOfFileStatus.length; i++)
        {
            Path localPath2 = arrayOfFileStatus[i].getPath();

            if (!localFileSystem.getFileStatus(localPath2).isDirectory()) // 不是目录文件
            {
                String str1 = localPath2.toString();
                String[] arrayOfString1 = str1.split("/");
                if (arrayOfString1[(arrayOfString1.length - 1)].substring(0, 5).equals("part-")) //对应输出格式
                {
                    System.err.println(str1);

                    FSDataInputStream localFSDataInputStream = localFileSystem.open(localPath2);
                    InputStreamReader localInputStreamReader = new InputStreamReader(localFSDataInputStream);
                    BufferedReader localBufferedReader = new BufferedReader(localInputStreamReader);
                    // POSTIVE NUM => (POSITIVE), (WORD), (POSITIVE，WORD) + NUM
                    // 统计三种频率
                    while ((str1 = localBufferedReader.readLine()) != null)
                    {
                        String[] arrayOfString2 = str1.split("\t");
                        Integer localInteger2 = new Integer(arrayOfString2[1]);

                        freq.put(arrayOfString2[0], localInteger2);

                        String str2 = arrayOfString2[0].substring(0, 1);
                        labelF.put(str2, (Integer) labelF.getOrDefault(str2, localInteger1) + localInteger2);

                        String str3 = arrayOfString2[0].substring(1);
                        wordF.put(str3, (Integer) wordF.getOrDefault(str3, localInteger1) + localInteger2);
                    }
                    localBufferedReader.close();
                    localInputStreamReader.close();
                    localFSDataInputStream.close();
                }
            }
        }
    }
}

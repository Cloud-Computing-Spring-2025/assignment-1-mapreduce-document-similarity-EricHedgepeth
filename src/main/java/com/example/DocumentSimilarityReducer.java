package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.*;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> docList = new ArrayList<>();
        for (Text doc : values) {
            String docStr = doc.toString();
            if (!docList.contains(docStr)) {
                docList.add(docStr);
            }
        }

        for (int i = 0; i < docList.size(); i++) {
            for (int j = i + 1; j < docList.size(); j++) {
                String docPair = docList.get(i) + "," + docList.get(j);
                context.write(new Text(docPair), key); // Emitting (doc1, doc2) -> common word
            }
        }
    }
}

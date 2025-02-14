package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {
    private Text wordKey = new Text();
    private Text docValue = new Text();

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String[] parts = value.toString().split("\t", 2);
        if (parts.length < 2) return;
        
        String document = parts[0];
        String[] words = parts[1].split(" ");
        Set<String> uniqueWords = new HashSet<>();

        for (String word : words) {
            uniqueWords.add(word);
        }
        
        for (String word : uniqueWords) {
            wordKey.set(word);
            docValue.set(document);
            context.write(wordKey, docValue); // Emitting (word, document)
        }
    }
}

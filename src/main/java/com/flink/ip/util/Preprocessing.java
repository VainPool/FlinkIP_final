package com.flink.ip.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Scanner;

public class Preprocessing {
    private String sourcePath;
    private String mysqlSafePath;
    private String mysqlUrl;
    private String mysqlDriverName;
    private String mysqlUserName;
    private String mysqlPassword;

    public Preprocessing(String sourcePath, String mysqlSafePath, String mysqlUrl, String mysqlDriverName, String mysqlUserName, String mysqlPassword) {
        this.sourcePath = sourcePath;
        this.mysqlSafePath = mysqlSafePath;
        this.mysqlUrl = mysqlUrl;
        this.mysqlDriverName = mysqlDriverName;
        this.mysqlUserName = mysqlUserName;
        this.mysqlPassword = mysqlPassword;
    }

    public ArrayList [] readAndShuffleAndTag(String filename) throws FileNotFoundException {
        Scanner sc = new Scanner(new File(sourcePath + filename));

        ArrayList [] result = new ArrayList[2];

        Integer rowCount = 0;
        ArrayList<String> fileContent = new ArrayList<>();
        while (sc.hasNextLine()){
            fileContent.add(sc.nextLine());
        }
        Collections.shuffle(fileContent);

        ArrayList<Tuple2<Integer,String>> withFilename = new ArrayList<>();
        ArrayList<String> noFilename = new ArrayList<>();

        for (String i : fileContent){
            withFilename.add(new Tuple2<>(rowCount%4, filename+"|"+i+rowCount%4));
            noFilename.add(i+rowCount%4);
            rowCount++;
        }

        result[0] = withFilename;
        result[1] = noFilename;

        return result;
    }

    public void writeToFile (String path, ArrayList<String> input, boolean appendOrNot, Integer j) throws IOException {
        Integer batch = 0;
        File file = new File(path);
        if(!file.exists()){
            file.createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(path, appendOrNot));
        for (String i : input){
            writer.write(i);
            writer.newLine();
            batch++;
            if(batch > 2000){
                writer.flush();
                batch = 0;
            }
        }

        if(path.equals("combinedFile/combinedInput.tbl")){
            Integer k = (25*(j+1));
            writer.write(k.toString()+"%");
            writer.newLine();
        }
        writer.close();
    }

    public String execute() throws Exception {
        // Check if sourcePath exists
        File test = new File(sourcePath);
        HashMap<Integer, ArrayList<String>> content = new HashMap<Integer, ArrayList<String>>();

        if(!test.exists()){
            return "sourcePath not exists";
        }

        // Prepare Mysql connection
        Class.forName(mysqlDriverName);
        Connection ct = DriverManager.getConnection(mysqlUrl,mysqlUserName,mysqlPassword);
        ct.setAutoCommit(false);
        Statement st = ct.createStatement();


        // Read, divide all files into 4 parts and shuffle
        for (File i : test.listFiles()) {
            String filename = i.getName();
            ArrayList [] allContent = readAndShuffleAndTag(filename);;
            ArrayList<Tuple2<Integer, String>> withFilename = allContent[0];
            for (Tuple2<Integer, String> j : withFilename){
                if (!content.containsKey(j.f0)){
                    content.put(j.f0, new ArrayList<String>());
                    content.get(j.f0).add(j.f1);
                }
                else{
                    content.get(j.f0).add(j.f1);
                }
            }

            // Prepare content to be imported into MySQL
            ArrayList<String> noFilename = allContent[1];
            String path = mysqlSafePath+filename;
            writeToFile(path,noFilename, false, 0);
            System.out.println("File "+filename+" prepared to be imported");

            // Load prepared data to MySQL
            System.out.println("load file " + filename.substring(0,filename.indexOf("."))+
                    " into MySQL... May take some time, please wait");

            st.executeLargeUpdate("load data infile '" + mysqlSafePath+filename+"' into table " + filename.substring(0,filename.indexOf("."))+
                    " fields terminated by '|' lines terminated by '\\n'");
            ct.commit();
        }
        st.close();
        ct.close();
        System.out.println("Importing to MySQL finished.");



        // Shuffle each part and write to one file
        for (Integer i : content.keySet()){
            Collections.shuffle(content.get(i));
            if (i == 0){
                writeToFile("combinedFile/combinedInput.tbl",content.get(i), false, i);
            }
            else{
                writeToFile("combinedFile/combinedInput.tbl",content.get(i), true, i);
            }
        }

        return "Preprocessing finished!";
    }


}

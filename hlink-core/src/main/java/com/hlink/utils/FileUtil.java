package com.hlink.utils;

import java.io.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

public class FileUtil {

    //读取json文件
    public static String readJsonFile(String fileName) {
        String jsonStr = "";
        try {
            File jsonFile = new File(fileName);
            FileReader fileReader = new FileReader(jsonFile);
            Reader reader = new InputStreamReader(new FileInputStream(jsonFile), "utf-8");
            int ch = 0;
            StringBuffer sb = new StringBuffer();
            while ((ch = reader.read()) != -1) {
                sb.append((char) ch);
            }
            fileReader.close();
            reader.close();
            jsonStr = sb.toString();
            return jsonStr;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }
    /**
     * 按行读取文件
     *
     * @param strFile
     * @return
     */
    public static List<String> readFileByLine(String strFile) {
        ArrayList<String> list = null;
        try {
            File file = new File(strFile);
            BufferedReader bufferedReader = new BufferedReader(new FileReader(file));
            String strLine = null;
            list = new ArrayList<String>();
            int lineCount = 1;
            while (null != (strLine = bufferedReader.readLine())) {
                list.add(strLine);
                lineCount++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return list;
    }

    // 12.枚举一个文件夹中的所有文件
    public static List<String> getAllFile(String dirName) {
        LinkedList<String> folderList = new LinkedList<String>();
        folderList.add(dirName);
        ArrayList<String> res = new ArrayList<>();
        while (folderList.size() > 0) {
            File file = new File(folderList.peek());
            folderList.removeLast();
            File[] files = file.listFiles();
            ArrayList<File> fileList = new ArrayList<File>();
            for (int i = 0; i < files.length; i++) {
                if (files[i].isDirectory()) {
                    folderList.add(files[i].getPath());
                } else {
                    fileList.add(files[i]);
                }
            }

            for (File f : fileList) {
                File filePath = f.getAbsoluteFile();
                res.add(filePath.getAbsolutePath());
            }

        }
        return res;
    }

    public static void writeListFile(List<String> strs, String fileName) {
        try {
            FileWriter fw = new FileWriter(fileName);
            for (String str : strs) {
                if (str != null) {
                    fw.write(str+"\n");
                }
            }
            fw.flush();
            fw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static HashSet<String> FileReader(String filename) {
        FileReader fr = null;
        HashSet<String> res = new HashSet<String>();
        try {
            fr = new FileReader(filename);
            BufferedReader bf = new BufferedReader(fr);
            String str;
            while ((str = bf.readLine()) != null) {
                res.add(str);
            }
            return res;
        } catch (IOException e) {
            System.out.println("read-Exception :" + e.toString());
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    System.out.println("close-Exception :" + e.toString());
                }
            }
        }
        return  null;
    }

    public static List<String> FileReaderList(String filename) {
        FileReader fr = null;
        List<String> res = new ArrayList<String>();
        try {
            fr = new FileReader(filename);
            BufferedReader bf = new BufferedReader(fr);
            String str;
            while ((str = bf.readLine()) != null) {
                res.add(str);
            }
            return res;
        } catch (IOException e) {
            System.out.println("read-Exception :" + e.toString());
        } finally {
            if (fr != null) {
                try {
                    fr.close();
                } catch (IOException e) {
                    System.out.println("close-Exception :" + e.toString());
                }
            }
        }
        return  null;
    }
    /*
     * 将person对象保存到文件中
     * params:
     * p:person类对象
     */
    public static void saveObjToFile(Object p,String fileName){
        try {
            //写对象流的对象
            ObjectOutputStream oos=new ObjectOutputStream(new FileOutputStream(fileName));

            oos.writeObject(p);         //将Person对象p写入到oos中

            oos.close();            //关闭文件流
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    /*
     * 从文件中读出对象，并且返回Person对象
     */
    public static Object getObjFromFile(String fileName){
        try {
            ObjectInputStream ois=new ObjectInputStream(new FileInputStream(fileName));

            Object person= ois.readObject();       //读出对象

            return person;                    //返回对象
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        return null;
    }
}

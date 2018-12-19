package com.distributesystem.springtest.utils;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author skye
 * @create 2018-12-17
 * @Description hdfs文件操作
 */

@Component
public class HdfsUtils {

    private ApplicationContext ctx;
    private FileSystem fileSystem;

    /**
     * 测试
     */
    public void test() {
        System.out.println("Hello!");
    }


    /**
     * 获取hdfs对象
     */
    private FileSystem getFileSystem() {

        ctx = new ClassPathXmlApplicationContext("beans.xml");
        fileSystem = (FileSystem) ctx.getBean("fileSystem");
        return fileSystem;

    }

    /**
     * 创建文件夹
     */
    public void mkdir(String path, String foldername) throws Exception {

        FileSystem fs = getFileSystem();
        String dir;
        if (path.substring(path.length() - 1).equals("/")) {
            dir = path + foldername;
        } else dir = path + "/" + foldername;
        Path scrpath = new Path(dir);
        boolean isOk = fs.mkdirs(scrpath);
        if (isOk) {
            System.out.println("create dir success!");
        } else {
            System.out.println("create dir failure...");
        }
        fs.close();

    }

    /**
     * 删除文件夹
     */
    public void delete(String path){

    }


//    /**
//     * 查看某个目录下的所有文件
//     */
//    public void listFiles(String filePath) throws Exception {
//
//        FileSystem fs = getFileSystem();
//        FileStatus[] fileStatuses = fs.listStatus(new Path(filePath));
//
//        for (FileStatus fileStatus : fileStatuses) {
//            String isDir = fileStatus.isDirectory() ? "文件夹" : "文件";
//            short replication = fileStatus.getReplication();
//            long len = fileStatus.getLen();
//            String path = fileStatus.getPath().toString();
//
//            System.out.println(isDir + "\t" + replication + "\t" + len + "\t" + path);
//        }
//    }

    /**
     * 遍历指定目录(direPath)下的所有文件
     */
    public List<Map<String, Object>> getDirectoryFromHdfs(String dirPath) {
        try {
            List<Map<String, Object>> fileInfoList = new ArrayList<Map<String, Object>>();
            FileSystem fs = getFileSystem();
            FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
            for (FileStatus fileStatus : fileStatuses) {
                Map<String, Object> map = new HashMap<String, Object>();
//                System.out.println("_________" + dirPath + "目录下所有文件______________");
                map.put("fileName", fileStatus.getPath().getName());
                map.put("fileSize", fileStatus.getLen());
                map.put("fileGroup", fileStatus.getGroup());
                map.put("fileOwner", fileStatus.getOwner());
                map.put("fileBlockSize", fileStatus.getBlockSize());
                map.put("filePermission", fileStatus.getPermission());
                map.put("fileReplication", fileStatus.getReplication());
                map.put("filePath", fileStatus.getPath());
                fileInfoList.add(map);
            }
            fs.close();
            return fileInfoList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 查看block信息
     */
    public List<Map<String, Object>> getBlockInfo(String file) {

        try {
            FileSystem fs = getFileSystem();
            FileStatus fst = fs.getFileStatus(new Path(file));
            if (fst.isDirectory()) return null;

            List<Map<String, Object>> blockInfoList = new ArrayList<>();

            BlockLocation[] blockLocations = fs.getFileBlockLocations(fst, 0, fst.getLen());
            for (int i = 0; i < blockLocations.length; i++) {

                BlockLocation blockLocation = blockLocations[i];

                Map<String, Object> map = new HashMap<>();
                map.put("blockId", i + 1);
//            map.put("blockName", blockLocation.getNames());
                map.put("blockSize", blockLocation.getLength());
                map.put("blockStartOffset", blockLocation.getOffset());

                String[] hosts = blockLocation.getHosts();
                String allhosts = hosts[0];
                for (int j = 1; j < hosts.length; j++) {
                    allhosts = allhosts + "/" + hosts[j];
                }
                map.put("nodeliststring", allhosts);

                blockInfoList.add(map);

//            String[] ids = blockLocation.getStorageIds();
//            for (String id : ids) {
//                System.out.println("id:" + id);
//            }
//
//            String[] names = blockLocation.getNames();
//            for (String name : names) {
//                System.out.println("name:" + name);
//            }
//
//            String[] hosts = blockLocation.getHosts();
//            for (String host : hosts) {
//                System.out.println("host:" + host);
//            }
//
//            System.out.println(blockLocation.getLength() + "/t" + blockLocation.getOffset());

            }
            fs.close();
            return blockInfoList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }


}

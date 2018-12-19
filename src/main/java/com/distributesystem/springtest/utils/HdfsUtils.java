package com.distributesystem.springtest.utils;

import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
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
     * 获取目标路径
     */
    private String getDistPath(String path, String filename) {

        String distPath;
        if (path.substring(path.length() - 1).equals("/")) {
            distPath = path + filename;
        } else distPath = path + "/" + filename;
        return distPath;

    }


    /**
     * 创建文件夹
     */
    public void mkdir(String path, String foldername) throws Exception {

        FileSystem fs = getFileSystem();
        Path scrpath = new Path(getDistPath(path, foldername));
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
    public void delete(String path, String deletedir) throws Exception {

        FileSystem fs = getFileSystem();
        boolean isok = fs.deleteOnExit(new Path(getDistPath(path, deletedir)));
        if (isok) {
            System.out.println("delete success!");
        } else {
            System.out.println("delete fail...");
        }
        fs.close();

    }


    /**
     * 遍历指定目录(direPath)下的所有文件
     */
    public List<Map<String, Object>> getDirectoryFromHdfs(String dirPath) {
        try {
            List<Map<String, Object>> fileInfoList = new ArrayList<>();
            FileSystem fs = getFileSystem();
            FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
            for (FileStatus fileStatus : fileStatuses) {
                Map<String, Object> map = new HashMap<>();
                map.put("fileName", fileStatus.getPath().getName());
                map.put("fileSize", fileStatus.getLen());
                map.put("fileGroup", fileStatus.getGroup());
                map.put("fileOwner", fileStatus.getOwner());
                map.put("fileBlockSize", fileStatus.getBlockSize());
                map.put("filePermission", fileStatus.getPermission().toString());
                map.put("fileReplication", fileStatus.getReplication());
                map.put("filePath", fileStatus.getPath().toString());
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
     * 上传文件
     */
    public void upLoad(String hdfspath, String localpath) throws Exception {
        FileSystem fs = getFileSystem();
        Path srcPath = new Path(localpath);
        Path dstPath = new Path(hdfspath);
        fs.copyFromLocalFile(srcPath, dstPath);
    }


    /**
     * 查看节点信息
     */
    public List<Map<String, Object>> getNodeInfo() throws Exception {

        FileSystem fs = getFileSystem();

        // 获取分布式文件系统
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;

        List<Map<String, Object>> nodeInfoList = new ArrayList<>();

        // 获取所有节点
        DatanodeInfo[] datanodeStats = hdfs.getDataNodeStats();

        for (int i = 0; i < datanodeStats.length; i++) {
            DatanodeInfo datanodeStat = datanodeStats[i];
            Map<String, Object> map = new HashMap<>();
            map.put("nodeId", "Node_" + i);
            map.put("nodeHostname", datanodeStat.getHostName());
            map.put("nodeIp", datanodeStat.getIpAddr());
            nodeInfoList.add(map);
        }
        hdfs.close();
        fs.close();
        return nodeInfoList;
    }


    /**
     * 查看block信息
     */
    public List<Map<String, Object>> getBlockInfo(String path, String filename) {

        try {
            FileSystem fs = getFileSystem();
            String file = getDistPath(path, filename);
            FileStatus fst = fs.getFileStatus(new Path(file));
            if (fst.isDirectory()) return null;

            List<Map<String, Object>> blockInfoList = new ArrayList<>();

            BlockLocation[] blockLocations = fs.getFileBlockLocations(fst, 0, fst.getLen());
            for (int i = 0; i < blockLocations.length; i++) {

                BlockLocation blockLocation = blockLocations[i];

                Map<String, Object> map = new HashMap<>();
                map.put("blockId", "Block_" + i);
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

            }
            fs.close();
            return blockInfoList;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;

    }


}

package com.distributesystem.hdfs.controller;

import com.distributesystem.hdfs.utils.HdfsUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * @author skye
 * @create 2018-12-17
 * @Description
 */

@RestController
public class HdfsController {

    @Autowired
    private HdfsUtils hdfsUtils;

    /**
     * 测试
     */
    @GetMapping("/hdfs/test")
    public void HdfsTest() {
        hdfsUtils.test();
    }


    /**
     * 创建hdfs文件夹
     */
    @PostMapping("/newfolder")
    public void HdfsMkdir(@RequestParam("path") String path,
                          @RequestParam("foldername") String foldername) throws Exception {

        hdfsUtils.mkdir(path, foldername);

    }

    /**
     * 删除文件夹
     */
    @PostMapping("/deletedir")
    public void HdfsDelete(@RequestParam("path") String path,
                           @RequestParam("deletedir") String deletedir) throws Exception {

        hdfsUtils.delete(path, deletedir);

    }

    /**
     * 查看某个目录下的所有文件
     */
    @PostMapping("/getfilelist")
    public List<Map<String, Object>> HdfsCatDir(@RequestParam("path") String filepath) {

        return hdfsUtils.getDirectoryFromHdfs(filepath);

    }

    /**
     * 上传文件
     */
    @PostMapping("/uploadfile")
    public void HdfsUpLoadFiles(@RequestParam("hdfspath") String hdfspath,
                                @RequestParam("localpath") String localpath) throws Exception {
        hdfsUtils.upLoad(hdfspath, localpath);
    }

    /**
     * 查看节点信息
     */
    @PostMapping("/getdatanodes")
    public List<Map<String, Object>> HdfsNodeInfo() throws Exception {

        return hdfsUtils.getNodeInfo();

    }

    /**
     * 查看block信息
     */
    @PostMapping("/getfileblockinfo")
    public List<Map<String, Object>> HdfsBlockInfo(@RequestParam("path") String filepath,
                                                   @RequestParam("filename") String filename) {

        return hdfsUtils.getBlockInfo(filepath, filename);

    }


}

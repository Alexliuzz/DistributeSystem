package com.distributesystem.springtest.controller;

import com.distributesystem.springtest.utils.HdfsUtils;
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
    public void HdfsMkdir(@RequestParam("path") String path, @RequestParam("foldername") String foldername) throws Exception {

        hdfsUtils.mkdir(path, foldername);

    }

//    /**
//     * 查看某个目录下的所有文件
//     */
//    @GetMapping("/getfilelist")
//    public void HdfsCatDir(@RequestParam("filepath") String filepath) throws Exception {
//
//        hdfsUtils.listFiles(filepath);
//
//    }

    /**
     * 查看某个目录下的所有文件
     */
    @PostMapping("/getfilelist")
    public List<Map<String, Object>> HdfsCatDir(@RequestParam("path") String filepath) {

        return hdfsUtils.getDirectoryFromHdfs(filepath);

    }

    /**
     * 查看block信息
     */
    @PostMapping("/getfileblockinfo")
    public List<Map<String, Object>> HdfsBlockInfo(@RequestParam("path") String filepath) throws Exception {

        return hdfsUtils.getBlockInfo(filepath);

    }


}

package com.opensds.utils;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.*;

public class Utils {

    /**
     * format strings for the date/time and date stamps required during signing
     **/
    public static final String ISO8601BasicFormat = "yyyyMMdd'T'HHmmss'Z'";

    /**
     * Get List of files
     *
     * @param beginPattern begin pattern (bucket) e.g: bucket_b237
     * @param path folder path e.g: resource/inputs
     * @return list of files
     */
    public static List<File> listFilesMatchingBeginsWithPatternInPath(final String beginPattern, String path) {
        List<File> retFileList = new ArrayList<>();
        try {
            File dir = new File(path);
            assert dir.isDirectory() : "Invalid directory path: "+path;
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.matches("^" + beginPattern + "+[a-z_1-9-]*.json");
                }
            });

            for (File xmlfile : files) {
                retFileList.add(xmlfile);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retFileList;
    }

    /**
     * Read file content.
     *
     * @param file file.
     * @return file content.
     */
    public static String  readFileContentsAsString(File file) {
        String content = null;
        try {
            content = new String(Files.readAllBytes(file.toPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }

    /**
     * Get Bucket Name.
     *
     * @param bucketFile file path
     * @return bucket name
     */
    public static String getBucketName(File bucketFile){
        return bucketFile.getName().substring(bucketFile.getName().indexOf("_") + 1,
                bucketFile.getName().indexOf("."));
    }

    /**
     * Get date.
     *
     * @return date.
     */
    public static String getDate() {
        Date now = new Date();
        SimpleDateFormat dateTimeFormat = new SimpleDateFormat(ISO8601BasicFormat);
        dateTimeFormat.setTimeZone(new SimpleTimeZone(0, "UTC"));
        return dateTimeFormat.format(now);
    }

    /**
     * Get Host
     *
     * @param url url e.g: http://192.168.34.45:6566
     * @return host e.g: 192.168.34.45:6566
     */
    public static String getHost(String url){
        String host = null;
        try {
            host = new URI(url).getHost()+":"+new URI(url).getPort();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        }
        return host;
    }

    /**
     * Generate random name.
     *
     * @param name any name
     * @return name
     */
    public static String getRandomName(String name){
        Random rand = new Random();
        int randInt = rand.nextInt(10000);
        return name+randInt;
    }
}

package com.opensds;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class Utils {
    public static List<File> listFilesMatchingBeginsWithPatternInPath(final String beginPattern, String path) {
        List<File> retFileList = new ArrayList<>();
        try {
            File dir = new File(path);
            File[] files = dir.listFiles(new FilenameFilter() {
                @Override
                public boolean accept(File dir, String name) {
                    return name.matches("^" + beginPattern + "+[a-z_1-9-]*.json");
                }
            });

            for (File xmlfile : files) {
                retFileList.add(xmlfile);
                //System.out.println(xmlfile);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return retFileList;
    }

    public static String  readFileContentsAsString(File file) {
        String content = null;
        try {
            content = new String(Files.readAllBytes(file.toPath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return content;
    }
}

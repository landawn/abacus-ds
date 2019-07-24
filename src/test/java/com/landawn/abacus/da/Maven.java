package com.landawn.abacus.da;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.landawn.abacus.util.IOUtil;
import com.landawn.abacus.util.StringUtil;
import com.landawn.abacus.util.function.BiPredicate;
import com.landawn.abacus.util.function.Consumer;
import com.landawn.abacus.util.function.Function;
import com.landawn.abacus.util.stream.Stream;
import com.landawn.abacus.util.stream.Stream.StreamEx;

public class Maven {

    public static void main(String[] args) throws Exception {
        final String sourceVersion = "0.0.1-SNAPSHOT";
        final String targetVersion = StreamEx.lines(new File("../abacus-da/build.properties"))
                .filter(line -> line.indexOf("<version>") > 0 && line.indexOf("</version>") > 0)
                .first()
                .map(line -> StringUtil.findAllSubstringsBetween(line, "<version>", "</version>").get(0))
                .get();
        final String commonMavenPath = "../abacus-da/maven/";
        final String sourcePath = commonMavenPath + sourceVersion;
        final String targetPath = commonMavenPath + targetVersion;
        final File sourceDir = new File(sourcePath);
        final File targetDir = new File(targetPath);

        IOUtil.deleteAllIfExists(targetDir);

        targetDir.mkdir();

        IOUtil.copy(sourceDir, targetDir);

        Stream.of(IOUtil.listFiles(targetDir, true, new BiPredicate<File, File>() {
            @Override
            public boolean test(File parentDir, File file) {
                // TODO Auto-generated method stub
                return file.getName().endsWith(".asc") || file.getName().endsWith("bundle.jar");
            }
        })).forEach(new Consumer<File>() {
            @Override
            public void accept(File t) {
                t.delete();
            }
        });

        Stream.of(IOUtil.listFiles(targetDir)).forEach(new Consumer<File>() {
            @Override
            public void accept(File file) {
                IOUtil.renameTo(file, file.getName().replace(sourceVersion, targetVersion));
            }
        });

        final List<File> files = IOUtil.listFiles(new File("./target/"));
        Stream.of("abacus-da")
                .map(new Function<String, String>() {
                    @Override
                    public String apply(String t) {
                        return t + "-" + targetVersion;
                    }
                })
                .forEach(new Consumer<String>() {
                    @Override
                    public void accept(final String n) {
                        Stream.of(files).forEach(new Consumer<File>() {
                            @Override
                            public void accept(File f) {
                                IOUtil.copy(f, targetDir);
                                IOUtil.renameTo(new File(targetDir + "/" + f.getName()), n + "-" + f.getName());
                            }
                        });
                    }
                });

        Stream.of(IOUtil.listFiles(targetDir, true, new BiPredicate<File, File>() {
            @Override
            public boolean test(File parentDir, File file) {
                // TODO Auto-generated method stub
                return file.getName().endsWith(".pom") || file.getName().endsWith(".xml") || file.getName().endsWith(".txt");
            }
        })).forEach(new Consumer<File>() {
            @Override
            public void accept(File t) {
                final List<String> lines = IOUtil.readLines(t);
                final List<String> newLines = new ArrayList<>(lines.size());
                for (String line : lines) {
                    newLines.add(line.replaceAll(sourceVersion, targetVersion));
                }
                IOUtil.writeLines(t, newLines);
            }
        });
    }

}

package edu.iu.dsc.tws.mpiapps.configuration.section;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.IntStream;

public class SmartHomeSection {


    public SmartHomeSection(String configurationFilePath) {
        Properties p = new Properties();
        try {
            p.load(new FileInputStream(configurationFilePath));
            dataFile = getProperty(p, "DataFile", "samlpe.csv");

        } catch (IOException e) {
            throw new RuntimeException("IO exception occurred while reading configuration properties file", e);
        }
    }

    private static String getProperty(Properties p, String name, String def) {
        String val = System.getProperty(name);
        if (val == null) {
            if (def != null) {
                val = p.getProperty(name, def);
            } else {
                val = p.getProperty(name);
            }
        }
        return val;
    }

    public String dataFile;


    private String getPadding(int count, String prefix){
        StringBuilder sb = new StringBuilder(prefix);
        IntStream.range(0,count).forEach(i -> sb.append(' '));
        return sb.toString();
    }

    public String toString(boolean centerAligned) {
        String[] params = {"DataFile"};
        Object[] args =
            new Object[]{dataFile};

        java.util.Optional<Integer> maxLength =
            Arrays.stream(params).map(String::length).reduce(Math::max);
        if (!maxLength.isPresent()) { return ""; }
        final int max = maxLength.get();
        final String prefix = "  ";
        StringBuilder sb = new StringBuilder("Parameters...\n");
        if (centerAligned) {
            IntStream.range(0, params.length).forEach(
                i -> {
                    String param = params[i];
                    sb.append(getPadding(max - param.length(), prefix))
                      .append(param).append(": ").append(args[i]).append('\n');
                });
        }
        else {
            IntStream.range(0, params.length).forEach(
                i -> {
                    String param = params[i];
                    sb.append(prefix).append(param).append(':')
                      .append(getPadding(max - param.length(), ""))
                      .append(args[i]).append('\n');
                });
        }
        return sb.toString();
    }
}



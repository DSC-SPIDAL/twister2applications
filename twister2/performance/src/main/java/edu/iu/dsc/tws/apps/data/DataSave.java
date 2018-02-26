package edu.iu.dsc.tws.apps.data;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.List;

public class DataSave {
  public static void saveList(String fileName, List<Long> list) throws FileNotFoundException {
    PrintWriter pw = new PrintWriter(new FileOutputStream(fileName));
    for (Long i : list) {
      pw.println(i);
    }
    pw.close();
  }
}

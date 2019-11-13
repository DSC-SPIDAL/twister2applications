package edu.iu.dsc.tws.flinkapps.data;

import edu.iu.dsc.tws.flinkapps.util.GetInfo;

import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class CollectiveData {
    private int[] list;

    private Random random;

    private String meta;

    private int iteration;

    private String iterationString;

    private Long messageTime;

    public CollectiveData(int size) {
        random = new Random();
        list = new int[size];
        meta = hostInfo();
        messageTime = System.nanoTime();
        for (int i = 0; i < size; i++) {
            list[i] = (random.nextInt());
        }
    }

    public CollectiveData(int size, int iteration) {
        this.iteration = iteration;
        random = new Random();
        list = new int[size];
        meta = GetInfo.hostInfo();
        this.messageTime = System.nanoTime();
        for (int i = 0; i < size; i++) {
            list[i] = (random.nextInt());
        }
    }

    public CollectiveData() {
    }

    public CollectiveData(int[] list) {
        this.list = list;
    }

    public CollectiveData(int[] list, int iteration) {
        this.iteration = iteration;
        this.list = list;
    }

    public CollectiveData(int[] list, String iteration) {
        this.iterationString = iteration;
        this.list = list;
    }
    public int[] getList() {
        return list;
    }

    public void setList(int[] list) {
        this.list = list;
    }

    public String getMeta() {
        return meta;
    }

    public void setMeta(String meta) {
        this.meta = meta;
    }

    public int getIteration() {
        return iteration;
    }

    public void setIteration(int iteration) {
        this.iteration = iteration;
    }

  public Long getMessageTime() {
    return messageTime;
  }

  public void setMessageTime(Long messageTime) {
    this.messageTime = messageTime;
  }

    public String getIterationString() {
        return iterationString;
    }

    public void setIterationString(String iterationString) {
        this.iterationString = iterationString;
    }

    @Override
    public String toString() {
        return "CollectiveData{" +
                "list=" + list +
                '}';
    }

    public String getSummary() {
      String summary = null;
      summary = "source," + this.iteration + ", " + this.meta + "," + System.nanoTime();
      return summary;
    }

    public static String hostInfo() {
        InetAddress ip;
        String hostname = "null";
        String hostInfo = "null-hostinfo";
        try {
            ip = InetAddress.getLocalHost();
            hostname = ip.getHostName();
//            System.out.println("Your current IP address : " + ip);
//            System.out.println("Your current Hostname : " + hostname);
            hostInfo = ip + "-" + hostname;
        } catch (UnknownHostException e) {
            System.out.println(e.getMessage());
        }
        return hostInfo;
    }
}

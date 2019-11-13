package edu.iu.dsc.tws.flinkapps.util;

import java.net.InetAddress;
import java.net.UnknownHostException;

public final class GetInfo {

    private GetInfo() {

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

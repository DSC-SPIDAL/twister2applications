package edu.iu.dsc.tws.apps.slam.streaming.stats;

import com.sun.management.GarbageCollectionNotificationInfo;
import com.sun.management.GcInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.MemoryUsage;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class GCInformation {
    private Logger LOG = LoggerFactory.getLogger(GCInformation.class);

    private static GCInformation ourInstance = new GCInformation();

    public static GCInformation getInstance() {
        return ourInstance;
    }

    private GCInformation() {
        installGCMonitoring();
    }

    private Map<String, GCCounter> counters = new HashMap<String, GCCounter>();

    private Lock lock = new ReentrantLock();

    public GCCounter addCounter() {
        UUID idOne = UUID.randomUUID();
        GCCounter counter = new GCCounter(idOne.toString());
        lock.lock();
        try {
            counters.put(counter.getId(), counter);
            return counter;
        } finally {
            lock.unlock();
        }
    }

    public void removeCounter(GCCounter counter) {
        lock.lock();
        try {
            counters.remove(counter.getId());
        } finally {
            lock.unlock();
        }
    }

    private void installGCMonitoring() {
        //get all the GarbageCollectorMXBeans - there's one for each heap generation
        //so probably two - the old generation and young generation
        List<GarbageCollectorMXBean> gcbeans = ManagementFactory.getGarbageCollectorMXBeans();
        LOG.info("gcbean size()....................................." + gcbeans.size());
        //Install a notifcation handler for each bean
        for (java.lang.management.GarbageCollectorMXBean gcbean : gcbeans) {
            NotificationEmitter emitter = (NotificationEmitter) gcbean;
            //use an anonymously generated listener for this example
            // - proper code should really use a named class
            NotificationListener listener = new NotificationListener() {
                //implement the notifier callback handler
                @Override
                public void handleNotification(Notification notification, Object handback) {
                    //we only handle GARBAGE_COLLECTION_NOTIFICATION notifications here
                    if (notification.getType().equals(GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION)) {
                        //get the information associated with this notification
                        GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
                        //get all the info and pretty print it
                        long duration = info.getGcInfo().getDuration();
                        lock.lock();
                        try {
                            for (Map.Entry<String, GCCounter> e : counters.entrySet()) {
                                GCCounter counter = e.getValue();
                                String gctype = info.getGcAction();
                                if ("end of minor GC".equals(gctype)) {
                                    counter.addYoungGCTime(duration);
                                } else if ("end of major GC".equals(gctype)) {
                                    counter.addFullGCTime(duration);
                                } else {
                                    counter.addYoungGCTime(duration);
                                }
                            }
                        } finally {
                            lock.unlock();
                        }
                    }
                }
            };

            //Add the listener
            emitter.addNotificationListener(listener, null, null);
        }
    }
}
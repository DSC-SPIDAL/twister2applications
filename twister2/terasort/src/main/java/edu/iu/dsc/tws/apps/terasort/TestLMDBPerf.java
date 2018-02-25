package edu.iu.dsc.tws.apps.terasort;

import com.sun.jna.ptr.ByteByReference;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.memory.OperationMemoryManager;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManager;
import edu.iu.dsc.tws.data.memory.lmdb.LMDBMemoryManagerContext;
import edu.iu.dsc.tws.data.memory.utils.DataMessageType;
import edu.iu.dsc.tws.data.utils.MemoryDeserializer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.lmdbjava.*;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.nio.ByteBuffer.allocateDirect;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.lmdbjava.DbiFlags.MDB_CREATE;
import static org.lmdbjava.Env.create;
import static org.lmdbjava.ByteBufferProxy.PROXY_OPTIMAL;
import static org.lmdbjava.ByteBufferProxy.PROXY_SAFE;
import static org.lmdbjava.EnvFlags.MDB_NOSYNC;
import static org.lmdbjava.EnvFlags.MDB_WRITEMAP;
import static org.lmdbjava.EnvFlags.MDB_MAPASYNC;

/**
 * Created by pulasthi on 2/24/18.
 */
public class TestLMDBPerf {

    public static final long MAP_SIZE_LIMIT = 1024 * 1204 * 1000;
    public static final int MAX_DB_INSTANCES = 2;
    public static final int MAX_READERS = 12;

    public Env<ByteBuffer> env;
     public Dbi<ByteBuffer> db;

    public static void main(String[] args) {
        Path dataPath = new Path("/home/pulasthi/work/twister2/lmdbPerf");
        LMDBMemoryManager lmdbMemoryManager = new LMDBMemoryManager(dataPath);
        int opertionID = (int) System.currentTimeMillis();
        OperationMemoryManager operationMemoryManager = lmdbMemoryManager.addOperation(opertionID, DataMessageType.BYTE);
        TestLMDBPerf perf = new TestLMDBPerf();

        perf.testLMDBOpti(dataPath);
//        perf.testLMDBOpti(dataPath);
//        perf.testLMDBOpti(dataPath);
    }

    private void testLMDB(Path lmdbDataPath) {

        ByteBuffer rwKey;
        ByteBuffer rwVal;
        rwKey = allocateDirect(8);
        rwVal = allocateDirect(8);

        final File path = new File(lmdbDataPath.getPath());
        if (!path.exists()) {
            path.mkdirs();
        }

        this.env = create()
                .setMapSize(MAP_SIZE_LIMIT)
                .setMaxDbs(MAX_DB_INSTANCES)
                .setMaxReaders(MAX_READERS)
                .open(path);

        int iterations = 100;
        byte[] data = new byte[20];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        ByteBuffer dataBuffer = ByteBuffer.allocateDirect(20).put(data);
        ByteBuffer[] keys = new ByteBuffer[iterations];

        double randomDouble = 0.0;
        for (int i = 0; i < iterations; i++) {
            randomDouble = Math.random();
            keys[i] = ByteBuffer.allocateDirect(8);
            keys[i].putDouble(randomDouble);
        }

        long startTime = System.currentTimeMillis();

        // The database supports duplicate values for a single key
        this.db = env.openDbi("wqe", MDB_CREATE);

        int count = 0;
        for (ByteBuffer key : keys) {
            dataBuffer.rewind();
            key.flip();
            db.put(key, dataBuffer);
            count++;
        }
        long endTime = System.currentTimeMillis();
        System.out.printf("Total time taken in Millis : %d , total count : %d \n", (endTime - startTime), count);

        Txn<ByteBuffer> txn = this.env.txnRead();
        System.out.println(keys[0].limit() + " : " + keys[0].remaining());
//        keys[0].flip();
        ByteBuffer result = db.get(txn, keys[0]);
        result.flip();
        System.out.println(result.limit());
        txn.close();
    }


    private void testLMDBOpti(Path lmdbDataPath) {
//        BufferProxy<ByteBuffer> bufferProxy = PROXY_OPTIMAL;
        final int POSIX_MODE = 664;

        ByteBuffer rwKey;
        ByteBuffer rwVal;
        rwKey = allocateDirect(8);
        rwVal = allocateDirect(8);

        final File path = new File(lmdbDataPath.getPath());
        if (!path.exists()) {
            path.mkdirs();
        }

        final EnvFlags[] envFlags = envFlags(true, false);
        this.env = create()
                .setMapSize(MAP_SIZE_LIMIT)
                .setMaxDbs(MAX_DB_INSTANCES)
                .setMaxReaders(MAX_READERS)
                .open(path, POSIX_MODE, envFlags);

        int iterations = 500000;
        byte[] data = new byte[20];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) i;
        }

        ByteBuffer dataBuffer = ByteBuffer.allocateDirect(20).put(data);
        ByteBuffer[] keys = new ByteBuffer[iterations];

        double randomDouble = 0.0;
        for (int i = 0; i < iterations; i++) {
            randomDouble = Math.random();
            keys[i] = ByteBuffer.allocateDirect(8);
            keys[i].putDouble(randomDouble);
        }

        long startTime = System.currentTimeMillis();

        // The database supports duplicate values for a single key
        this.db = this.env.openDbi("wqe", MDB_CREATE);
        ;
        int count = 0;
        for (ByteBuffer key : keys) {
            dataBuffer.rewind();
            key.flip();
            db.put(key, dataBuffer);
            count++;
        }

        long endTime = System.currentTimeMillis();
                env.sync(true);

        System.out.printf("Total time taken in Millis Write : %d , total count : %d \n", (endTime - startTime), count);

        startTime = System.currentTimeMillis();
        ByteBuffer results;
        count = 0;
        Txn<ByteBuffer> txn = this.env.txnRead();
        try (CursorIterator<ByteBuffer> it = db.iterate(txn, KeyRange.all())) {
            for (final CursorIterator.KeyVal<ByteBuffer> kv : it.iterable()) {
                results = kv.val();
                count++;
            }
        }
        endTime = System.currentTimeMillis();
        System.out.printf("Total time taken in Millis Read : %d , total count : %d \n", (endTime - startTime), count);

        txn.close();

    }

    static final EnvFlags[] envFlags(final boolean writeMap, final boolean sync) {
        final Set<EnvFlags> envFlagSet = new HashSet<>();
        if (writeMap) {
            envFlagSet.add(MDB_WRITEMAP);
//            envFlagSet.add(EnvFlags.MDB_NOSYNC);
//            envFlagSet.add(EnvFlags.MDB_NOMETASYNC);
//            envFlagSet.add(MDB_MAPASYNC);

        }
        if (!sync) {
            envFlagSet.add(MDB_NOSYNC);
        }
        final EnvFlags[] envFlags = new EnvFlags[envFlagSet.size()];
        envFlagSet.toArray(envFlags);
        return envFlags;
    }

}

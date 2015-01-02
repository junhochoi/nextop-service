package io.nextop.service.overlord;

import com.google.gson.JsonObject;
import rx.Observable;

import java.util.Map;

/** Model for tracking performance metrics.
 * Thread-safe. */
public class Metrics {
    public static final class Config {

    }







    // FIXME start take in a config source



    /////// INPUT ///////

    /* input assumes the overlord maintains the round trip time and client time for each client */


    /** The speedup is estimated as different between the actual time (<code>clientTime + proxyTime</code>)
     * and the time that would happen if the client was re-creating the connection on each attempt
     * (<code>(clientAttempts + proxyAttempts) * (3 * clientRoundtripTime + clientTime + proxyTime)</code>).
     *
     * @param clientRoundtripTime median time of a roundtrip to the client
     * @param clientAttempts number of attempts between client and service
     * @param clientTime time spent transferring the request from client to service and back.
     *                   Does not include time spent maintaining the connection (disconnected, handshakes, etc.)
     * @param proxyAttempts number of attempts between service and external endpoint
     * @param proxyTime time spent transferring the successful request from service to external endpoint and back.
     *                  Does not include time maintaining the connection (disconnected, handshakes, etc.) */
    public void countSpeedup(long measurementTime, long clientRoundtripTime, int clientAttempts, long clientTime,
                            int proxyAttempts, long proxyTime) {
        // FIXME
    }

    public void countAck(long measurementTime, int ackCount) {
        // FIXME
    }

    // can be negative to indicate a transition from nack to ack.
    public void countNack(long measurementTime, int nackCount) {
        // FIXME
    }

    public void countData(long measurementTime, long bytes) {
        // FIXME
    }


    /////// SNAPSHOTS ///////

    public Observable<Snapshot> getMinutelySnapshots() {
        // FIXME subscribe to a minutely snapshot behavior
        // FIXME this is the same problem as the config watcher, that we want the poller to start on the first subscription to the behavior
        return null;
    }

    public Snapshot getSnapshot() {
        // FIXME
        return null;
    }

    public static final class Snapshot {
        public static final class Percentile {
            // FIXME
//            public final int percent;

            // FIXME equals, hashcode, toString
        }
        public static final class TimeBucket {
            // FIXME
            // UTC
//            public final long start;
//            public final long stop;

            // FIXME equals, hashcode, toString
        }

        public static final class Speedup {
            // FIXME
        }
        public static final class AckCount {
            // FIXME
        }
        public static final class DataCount {
            // FIXME
        }


        Map<Percentile, Speedup> speedups;
        Map<TimeBucket, AckCount> ackCounts;
        Map<TimeBucket, DataCount> dataCounts;
    }


}

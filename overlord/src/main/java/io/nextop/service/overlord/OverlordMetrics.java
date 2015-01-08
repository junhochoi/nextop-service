package io.nextop.service.overlord;

import com.google.common.collect.ImmutableMap;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import io.nextop.ApiComponent;
import rx.Observable;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

/** Model for tracking performance metrics for the overlord instance.
 * Thread-safe. */
// FIXME include session metrics here too
public class OverlordMetrics extends ApiComponent.Base {

    public static final class Config {

    }




    long initTime;
    long initNanos;


    public OverlordMetrics() {
        initTime = System.currentTimeMillis();
        initNanos = System.nanoTime();
    }


    private long now() {
        return initTime + (System.nanoTime() - initTime);
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
        // FIXME fill this with real data when available

        Map<Snapshot.Percentile, Snapshot.Speedup> speedups = new HashMap<>(100);
        Map<Snapshot.TimeBucket, Snapshot.MessageVolume> messageVolumes = new HashMap<>(8);
        Map<Snapshot.TimeBucket, Snapshot.DataVolume> dataVolumes = new HashMap<>(8);

        Random r = new Random();

        // speedups
        {
            int[] csteps = new int[101];
            csteps[0] = 0;
            for (int i = 1; i <= 100; ++i) {
                csteps[i] = csteps[i - 1] + r.nextInt(100);
            }
            float msSwing = 500.f;
            for (int i = 1; i <= 100; ++i) {
                speedups.put(new Snapshot.Percentile(i), new Snapshot.Speedup(csteps[i] * msSwing / csteps[100]));
            }
        }

        long[] timeSteps = new long[]{
                TimeUnit.MINUTES.toMillis(1),
                TimeUnit.MINUTES.toMillis(1),
                TimeUnit.MINUTES.toMillis(5),
                TimeUnit.MINUTES.toMillis(5),
                TimeUnit.MINUTES.toMillis(30),
                TimeUnit.MINUTES.toMillis(30),
                TimeUnit.HOURS.toMillis(1),
                TimeUnit.HOURS.toMillis(12),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(1),
                TimeUnit.DAYS.toMillis(7)
        };
        long[] ctimes = new long[1 + timeSteps.length];
        ctimes[0] = now();
        for (int i = 0; i < timeSteps.length; ++i) {
            ctimes[1 + i] = ctimes[i] + timeSteps[i];
        }
        Snapshot.TimeBucket[] timeBuckets = new Snapshot.TimeBucket[timeSteps.length];
        for (int i = 0; i < timeSteps.length; ++i) {
            timeBuckets[i] = new Snapshot.TimeBucket(ctimes[i], ctimes[i + 1]);
        }

        // message volumes
        {
            float messagesPerHour = 1000.f;
            int cnackedCount = r.nextInt(100);
            for (int i = 0; i < timeBuckets.length; ++i) {
                Snapshot.TimeBucket timeBucket = timeBuckets[i];

                float m = (timeBucket.endTime - timeBucket.startTime) * messagesPerHour / TimeUnit.HOURS.toMillis(1);

                // na is number of messages that got sent this period
                int na = Math.round(r.nextFloat() * m);
                // a is number of messages that got acked this period
                int a = Math.round(r.nextFloat() * (cnackedCount + na));
                cnackedCount = Math.max(0, cnackedCount + na - a);

                messageVolumes.put(timeBucket, new Snapshot.MessageVolume(a, cnackedCount));
            }
        }

        // data volumes
        {
            float bytesPerHour = 8 * 1024.f * 1024.f;
            for (int i = 0; i < timeBuckets.length; ++i) {
                Snapshot.TimeBucket timeBucket = timeBuckets[i];

                float m = (timeBucket.endTime - timeBucket.startTime) * bytesPerHour / TimeUnit.HOURS.toMillis(1);

                int b = Math.round(r.nextFloat() * m);

                dataVolumes.put(timeBucket, new Snapshot.DataVolume(b));
            }
        }

        return new Snapshot(speedups, messageVolumes, dataVolumes);
    }

    public static final class Snapshot {
        public final Map<Percentile, Speedup> speedups;
        public final Map<TimeBucket, MessageVolume> messageVolumes;
        public final Map<TimeBucket, DataVolume> dataVolumes;


        public Snapshot(Map<Percentile, Speedup> speedups,
                        Map<TimeBucket, MessageVolume> messageVolumes,
                        Map<TimeBucket, DataVolume> dataVolumes) {
            this.speedups = ImmutableMap.copyOf(speedups);
            this.messageVolumes = ImmutableMap.copyOf(messageVolumes);
            this.dataVolumes = ImmutableMap.copyOf(dataVolumes);
        }


        public JsonObject toJson() {
            JsonObject speedupsObject = new JsonObject();
            for (Map.Entry<Percentile, Speedup> e : new TreeMap<>(speedups).entrySet()) {
                speedupsObject.add(e.getKey().toString(), e.getValue().toJson());
            }
            JsonObject messageVolumesObject = new JsonObject();
            for (Map.Entry<TimeBucket, MessageVolume> e : new TreeMap<>(messageVolumes).entrySet()) {
                messageVolumesObject.add(e.getKey().toString(), e.getValue().toJson());
            }
            JsonObject dataVolumesObject = new JsonObject();
            for (Map.Entry<TimeBucket, DataVolume> e : new TreeMap<>(dataVolumes).entrySet()) {
                dataVolumesObject.add(e.getKey().toString(), e.getValue().toJson());
            }

            JsonObject object = new JsonObject();
            object.add("speedups", speedupsObject);
            object.add("messageVolumes", messageVolumesObject);
            object.add("dataVolumes", dataVolumesObject);
            return object;
        }


        public static final class Percentile implements Comparable<Percentile> {
            public final int percent;

            public Percentile(int percent) {
                this.percent = percent;
            }

            @Override
            public int compareTo(Percentile b) {
                return percent - b.percent;
            }

            @Override
            public String toString() {
                return String.format("%d%%", percent);
            }

            @Override
            public int hashCode() {
                return Integer.hashCode(percent);
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof Percentile)) {
                    return false;
                }
                Percentile b = (Percentile) obj;
                return percent == b.percent;
            }
        }

        public static final class TimeBucket implements Comparable<TimeBucket> {
            // UTC ms
            public final long startTime;
            public final long endTime;

            public TimeBucket(long startTime, long endTime) {
                this.startTime = startTime;
                this.endTime = endTime;
            }

            @Override
            public int compareTo(TimeBucket b) {
                int d = Long.compare(startTime, b.startTime);
                if (0 != d) {
                    return d;
                }
                d = Long.compare(endTime, b.endTime);
                return d;
            }

            @Override
            public String toString() {
                String fstartTime;
                String fendTime;
                synchronized (format) {
                    fstartTime = format.format(new Date(startTime));
                    fendTime = format.format(new Date(endTime));
                }
                return String.format("%d %s/%d %s", startTime, fstartTime, endTime, fendTime);
            }

            @Override
            public int hashCode() {
                int m = 31;
                int c = Long.hashCode(startTime);
                c = m * c + Long.hashCode(endTime);
                return c;
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof TimeBucket)) {
                    return false;
                }
                TimeBucket b = (TimeBucket) obj;
                return startTime == b.startTime && endTime == b.endTime;
            }

            private static final SimpleDateFormat format;
            static {
                format = new SimpleDateFormat("MM.dd.yyyy HH:mm:ss z");
                format.setTimeZone(TimeZone.getTimeZone("UTC"));
            }
        }

        public static final class Speedup {
            public final float msPerRequest;

            public Speedup(float msPerRequest) {
                // normalize the value to within 0.01
                this.msPerRequest = (int) (100 * msPerRequest) / 100.f;
            }

            public JsonElement toJson() {
                return new JsonPrimitive(msPerRequest);
            }

            @Override
            public String toString() {
                return String.format("%.2f", msPerRequest);
            }

            @Override
            public int hashCode() {
                return Float.hashCode(msPerRequest);
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof Speedup)) {
                    return false;
                }
                Speedup b = (Speedup) obj;
                return msPerRequest == b.msPerRequest;
            }
        }

        public static final class MessageVolume {
            public final long ackedCount;
            public final long nackedCount;

            public MessageVolume(long ackedCount, long nackedCount) {
                this.ackedCount = ackedCount;
                this.nackedCount = nackedCount;
            }

            public JsonElement toJson() {
                JsonObject object = new JsonObject();
                object.add("acked", new JsonPrimitive(ackedCount));
                object.add("nacked", new JsonPrimitive(nackedCount));
                return object;
            }

            @Override
            public String toString() {
                return String.format("%d/%d", ackedCount, nackedCount);
            }

            @Override
            public int hashCode() {
                int m = 31;
                int c = Long.hashCode(ackedCount);
                c = m * c + Long.hashCode(nackedCount);
                return c;
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof MessageVolume)) {
                    return false;
                }
                MessageVolume b = (MessageVolume) obj;
                return ackedCount == b.ackedCount && nackedCount == b.nackedCount;
            }
        }

        public static final class DataVolume {
            public final long byteCount;
            public final float mibCount;

            public DataVolume(long byteCount) {
                this.byteCount = byteCount;
                mibCount = byteCount / (1024.f * 1024.f);
            }

            public JsonElement toJson() {
                return new JsonPrimitive(mibCount);
            }

            @Override
            public String toString() {
                // int MiB
                return String.format("%.6f", mibCount);
            }

            @Override
            public int hashCode() {
                return Long.hashCode(byteCount);
            }

            @Override
            public boolean equals(Object obj) {
                if (!(obj instanceof DataVolume)) {
                    return false;
                }
                DataVolume b = (DataVolume) obj;
                return byteCount == b.byteCount;
            }
        }
    }
}

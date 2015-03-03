package io.nextop.server;

import io.nextop.*;
import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.concurrent.Executor;

/** v1 based on a server socket.
 */
// TODO move to nio
// FIXME(SECURITY) verify the clientID with the private certificate
// FIXME(SECURITY) not doing this is a huge security hole because cache control relies on an accurate clientID
public class NextopServer extends Observable<NextopSession> {

    public static final class Config {
        public final int port;
        public final int backlog;

        public Config(int port, int backlog) {
            this.port = port;
            this.backlog = backlog;
        }
    }


    final Config config;

    final Executor workExecutor;

    final PublishSubject<NextopSession> publishSession;


    @Nullable
    ControlLooper controlLooper = null;


    public NextopServer(Config config, Executor workExecutor) {
        this(config, workExecutor, PublishSubject.<NextopSession>create());
    }

    private NextopServer(Config config, Executor workExecutor, final PublishSubject<NextopSession> publishSession) {
        super(new OnSubscribe<NextopSession>() {
            @Override
            public void call(Subscriber<? super NextopSession> subscriber) {
                publishSession.subscribe(subscriber);
            }
        });

        this.config = config;
        this.workExecutor = workExecutor;
        this.publishSession = publishSession;
    }


    public void start() {
        if (null == controlLooper) {
            controlLooper = new ControlLooper();
            controlLooper.start();
        }
    }

    public void stop() {
        if (null != controlLooper) {
            controlLooper.close();
            controlLooper = null;
        }
    }


    final class ControlLooper extends Thread {
        boolean active = true;

        @Nullable
        ServerSocket serverSocket = null;

        void close() {
            active = false;
            if (null != serverSocket) {
                try {
                    serverSocket.close();
                } catch (IOException e) {
                    // FIXME log
                } finally {
                    serverSocket = null;
                }
            }
        }

        @Override
        public void run() {
            System.out.printf("STARTING CONTROL LOOPER ON port %d\n", config.port);

            try {
                serverSocket = new ServerSocket(config.port, config.backlog);
            } catch (Exception e) {
                // FIXME log
                e.printStackTrace();
                active = false;
                return;
            }
            try {
                while (active) {
                    Socket socket;
                    try {
                        System.out.printf("WAITING FOR SOCKET\n");
                        socket = serverSocket.accept();
                        // socket.setTcpNoDelay(false);
                    } catch (IOException e) {
                        // FIXME log
                        // fatal
                        active = false;
                        return;
                    }
                    System.out.printf("GOT SOCKET\n");

                    workExecutor.execute(new GreetingWorker(socket));
                }
            } finally {
                close();
            }
        }
    }

    final class GreetingWorker implements Runnable {
        final Socket socket;

        byte[] greetingBuffer = new byte[1024];


        @Nullable
        Id clientId;
        Id sessionId = Id.create();

        @Nullable
        Message greeting = null;


        GreetingWorker(Socket socket) {
            this.socket = socket;
        }


        @Override
        public void run() {
            try {
                try {

                    {
                        long startNanos = System.nanoTime();
                        System.out.printf("READ GREETING\n");
                        readGreeting(socket.getInputStream());
                        System.out.printf("WRITE GREETING\n");
                        writeGreetingResponse(socket.getOutputStream());

                        System.out.printf("Greeting took %.3fms\n", ((System.nanoTime() - startNanos) / 1000) / 1000.f);
                    }

                    Socket tlsSocket = startTls(socket);

                    Wire wire = Wires.io(tlsSocket);

                    // at this point the session is verified and active
                    publishSession.onNext(new NextopSession(clientId, sessionId, wire));
                } catch (Exception e) {
                    // FIXME log
                    socket.close();
                }
            } catch (IOException e) {
                // FIXME log
            }
        }

        /** pair to {@link NextopClientWireFactoryNode#writeGreeting} */
        private void readGreeting(InputStream is) throws IOException {
            int i = 0;
            for (int r; 0 < (r = is.read(greetingBuffer, i, 2 - i)); ) {
                i += r;
            }
            if (i < 2) {
                throw new IOException();
            }

            int length = ((0xFF & greetingBuffer[0]) << 8) | (0xFF & greetingBuffer[1]);
            if (greetingBuffer.length < length) {
                throw new IOException("Greeting response too long.");
            }

            i = 0;
            for (int r; 0 < (r = is.read(greetingBuffer, i, length - i)); ) {
                i += r;
            }
            if (i < length) {
                throw new IOException();
            }

            WireValue greetingValue = WireValue.valueOf(greetingBuffer);
            switch (greetingValue.getType()) {
                case MESSAGE:
                    handleGreeting(greetingValue.asMessage());
                    break;
                default:
                    throw new IOException("Bad greeting.");
            }

        }

        /** pair to {@link NextopClientWireFactoryNode#readGreetingResponse} */
        private void writeGreetingResponse(OutputStream os) throws IOException {
            Message greeting = Message.newBuilder()
                    .setRoute(Route.create(Route.Target.valueOf("PUT /greeting-response"), Route.LOCAL))
                    .set("sessionId", sessionId)
                    .build();

            ByteBuffer bb = ByteBuffer.wrap(greetingBuffer, 2, greetingBuffer.length - 2);
            WireValue.of(greeting).toBytes(bb);
            bb.flip();

            int length = bb.remaining();
            greetingBuffer[0] = (byte) (length >>> 8);
            greetingBuffer[1] = (byte) length;

            os.write(greetingBuffer, 0, 2 + bb.remaining());
            os.flush();
        }


        private void handleGreeting(Message greeting) throws IOException {
            // FIXME(security) verify that the client certificate in the greeting matches the certificate
            // FIXME(security) for the clientId
            @Nullable WireValue clientIdValue = greeting.parameters.get(WireValue.of("clientId"));
            if (null == clientIdValue) {
                throw new IOException("Greeting missing client ID.");
            }
            clientId = clientIdValue.asId();

            this.greeting = greeting;
        }


        private Socket startTls(Socket socket) {
            // FIXME
            return socket;
        }
    }


    /*

io.nextop.server architecture:
- Observable<NextopSession>
- base on a socket channel
- on connect, negotiate the greeting, get the client id, the publish(WireWithClientId)
- on new Wire, connect to an existing NextopNode, or create a new Head chain

     */

}

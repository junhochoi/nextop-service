package io.nextop.server;

public class NextopServer {

    /*

io.nextop.server architecture:
- Observable<NextopSession>
- base on a socket channel
- on connect, negotiate the greeting, get the client id, the publish(WireWithClientId)
- on new Wire, connect to an existing NextopNode, or create a new Head chain

     */
}

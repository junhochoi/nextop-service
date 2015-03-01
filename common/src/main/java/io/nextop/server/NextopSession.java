package io.nextop.server;

import io.nextop.Id;
import io.nextop.client.Wire;

public final class NextopSession {
    public final Id clientId;
    public final Id sessionId;
    public final Wire wire;


    public NextopSession(Id clientId, Id sessionId, Wire wire) {
        this.clientId = clientId;
        this.sessionId = sessionId;
        this.wire = wire;
    }
}

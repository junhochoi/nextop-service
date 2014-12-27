Nextop Service


Principles
==========

- Distributed with single masters (hyperlord, overlord)
- One hyperlord
- One overlord per access key
- Billing and metrics are done per access key
- Cloud agnostic: masters run in a single data center, but edges are distributed across data centers
- All edge sessions are double-encrypted. Each edge session has a unique double-encryption keypair. (data is wrapped in the session keypair then in the nextop keypair)
- All IDs, access keys, grant keys are 256-bit UUIDs. The web layer ensures uniqueness of access keys to a single account with a transactional DB. The hyperlord ensures uniqueness of access keys to overlord but cannot ensure that multiple accounts aren't sharing the same access key.

Topology
========

- Seperate domains per access key ($access-key.nextop.io). Overlord and edge instances per domain.
- DNS for overlords is managed by the hyperlord (not a DNS server). For communication to an overlord, use the authority provided by the hyperlord. On HTTP, pass the correct Host header.
- Web and hyperlord hosted in AWS
- Hyperlord is reachable only from AWS subnet
- Overlords are established by the hyperlord. A NXT connection to the overlord is used to control the overlord (hyperlord reaches out)


Edge Organization
=================

Currently the overload is the only edge. 

Eventually the overlord will manage multiple edges across multiple clouds. The overlord collaborates with the edges using a simple protocol.

- Move session. Seamlessly moves a session from one edge to another. The two edges communicate with each other to orchestrate the move.
- Shutdown. Directs the edge to shutdown when its last session completes.

NXT connections directly to the edge create sessions. NXT has a move message that tells the client to connect to a different edge (and re-send all unacked messages).


Interfaces
==========

## HTTP 1.1/2

- Only the hyperlord and overlord accept HTTP in.
- Authentication over HTTP connections via the parameter grant-key=$grant-key . Use + to apply multiple grant keys.

## NXT

- nextop-client speaks NXT

## Benefits of NXT over HTTP

- Use sync logic for sending
- Double-encryption
- Rx: Subscribe to arbitrary paths (un-authed clients can still receive on message IDs that they send)
- Rx: cancel, complete
- Reliability: ack, nack

Proxy
=====

Outbound connections are done via HTTP 1.1/2 .

Configuration and Metric API
============================

The web console and email system will use these. 

API methods are idempotent.

## Hyperlord

Because the hyperlord is reachable only on the AWS subnet, hyperlord API methods do not require grant keys.

### PUT https://hyperlord.nextop.io/$access-key?set-root-grant-key=$root-grant-key

Create an access key and assign the given admin grant key to it. This call blocks until the overlord is active. If the overlord already exists with a different root grant key, signals an error.

Returns the overlord authority.

### DELETE https://hyperlord.nextop.io/$access-key

Deletes the access key. This sends an email to ops, who has to manually click a link to confirm the deletion. Clicking the link is a hard shutdown. 

The web console should be a layer above this. It should go through an email confirmation process with the account owner, which will then cascade into a hyperlord delete.

## DNS

### GET https://dns.nextop.io/$access-key/overlord

Requires admin grant key.

Replaces DNS for host "$access-key.nextop.io".

Returns overlord authorities, semicolon separated. Overlords can be used interchangeably.

### GET https://dns.nextop.io/$access-key/edge

Client access points for "$access-key.nextop.io".

Returns edge authorities, semicolon separated. Edges can be used interchangeably.

## Overlord


### PUT https://$access-key.nextop.io/grant-key/$grant-key?$permission-name=$permission-value

Requires admin grant key.

Create a grant key. Supply one or more permission names. If the grant key already exists with different permissions, signals an error.


### POST https://$access-key.nextop.io/grant-key/$grant-key?$permission-name=$permission-value

Requires admin grant key.

Sets the permissions for the grant key. Supply one or more permission names. 


### DELETE https://$access-key.nextop.io/grant-key/$grant-key

Requires admin grant key.

Revoke a grant key.


### GET https://$access-key.nextop.io/metrics

Requires monitor grant key.

Returns a JSON object with a snapshot of metrics.


### GET https://$access-key.nextop.io/config

Returns a JSON object with all keys filled.


### POST https://$access-key.nextop.io/config

Accepts a JSON object with only the keys to update filled with the new values.


Grant Key Permissions
=====================

(replace camel case with - when used in URI params)

- admin (bool)
- monitor (bool)
- send (bool)
- subscribe (bool)







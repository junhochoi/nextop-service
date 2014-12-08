nextop-proxy
============

The proxy accepts incoming requests on ports 80 (HTTP1.1, HTTP/2) and port 180 (NXT). Incoming requests are either proxied or routed on NXT connections. The decision of proxy versus route is a state machine depending on the configuration, current receivers, and error codes of the proxied request.

Clients that auth establish a NXT connection. The NXT connection is logically to the host $accesskey.nextop.io. The NXT connection enables:
- use sync logic for sending
- receive arbitrary paths (un-authed clients can still receive on message IDs that they send)
- cancel, complete, ack, nack

The proxy is configured via a RESTful API. This must be done on the NXT connection with the nextop-admin access key and grant key. Typically the web console acts as the bridge between a user and nextop-admin, handling the authentication and usability issues.
- PUT https://nextop.io/account/$accesskey
- DELETE https://nextop.io/account/$accesskey

Each access key can be configured via a RESTful API. GET returns all keys in a JSON object; POST accepts a JSON object with just keys of the values to be updates.
- GET https://$accesskey.nextop.io/config
- POST https://$accesskey.nextop.io/config






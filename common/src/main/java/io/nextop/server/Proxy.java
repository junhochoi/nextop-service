package io.nextop.server;

import io.nextop.client.node.Head;

public class Proxy {

    Head in;
    Head out;

    // FIXME defaultSubscriber on the in
    // FIXME route to out

    // FIXME on in -> (request) -> out
    // FIXME intercept and do:
    // FIXME  - memcache

    // FIXME on out -> (response) -> in
    // FIXME intercept response and:
    // FIXME memcache
    // FIXME - if request had image layer pragmas, do downsampling

}

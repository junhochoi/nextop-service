package io.nextop.service.m;

import io.nextop.Authority;
import io.nextop.Ip;
import it.unimi.dsi.fastutil.ints.IntSet;

public final class BadAuthority {
    public Ip reporter;
    public Authority authority;
    public IntSet schemes;
}

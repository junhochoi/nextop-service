package io.nextop.service;

public final class Authority {
    public final String host;
    public final int port;


    public Authority(String host, int port) {
        this.host = host;
        this.port = port;
    }


    @Override
    public String toString() {
        return String.format("%s:%d", host, port);
    }


    /////// GENERATED ///////

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Authority authority = (Authority) o;

        if (port != authority.port) return false;
        if (!host.equals(authority.host)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = host.hashCode();
        result = 31 * result + port;
        return result;
    }
}

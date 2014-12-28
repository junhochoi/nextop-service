package io.nextop.service;

/** {@link #toString} must match the values in GrantKeyPermission.permission_name */
public enum Permission {
    admin,
    monitor,
    send,
    subscribe;

    public Mask on() {
        return new Mask(this, true);
    }

    public Mask off() {
        return new Mask(this, false);
    }

    public static final class Mask {
        public final Permission p;
        public final boolean mask;

        public Mask(Permission p, boolean mask) {
            this.p = p;
            this.mask = mask;
        }
    }

}

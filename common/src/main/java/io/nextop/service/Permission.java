package io.nextop.service;

/** {@link #toString} must match the values in GrantKeyPermission.permission_name */
public enum Permission {
    admin,
    monitor,
    send,
    subscribe;

    public Mask mask(boolean mask) {
        return new Mask(this, mask);
    }

    public Mask on() {
        return new Mask(this, true);
    }

    public Mask off() {
        return new Mask(this, false);
    }

    public static final class Mask {
        public final Permission permission;
        public final boolean mask;

        public Mask(Permission permission, boolean mask) {
            this.permission = permission;
            this.mask = mask;
        }

        @Override
        public String toString() {
            return String.format("%s=%b", permission, mask);
        }


        /////// GENERATED ///////

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Mask mask1 = (Mask) o;

            if (mask != mask1.mask) return false;
            if (permission != mask1.permission) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = permission.hashCode();
            result = 31 * result + (mask ? 1 : 0);
            return result;
        }
    }
}

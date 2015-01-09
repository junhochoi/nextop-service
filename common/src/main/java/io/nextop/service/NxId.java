package io.nextop.service;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.UUID;

/** 256-bit UUID.
 * @see #create */
public final class NxId {
    private static final SecureRandom sr = new SecureRandom();

    /** generate a 256-bit UUID as a 128-bit UUID + 128 bits of randomness
     * @see java.util.UUID */
    public static NxId create() {
        UUID uuid16 = UUID.randomUUID();
        byte[] rand16 = new byte[16];
        sr.nextBytes(rand16);

        return new NxId(ByteBuffer.allocate(32
        ).putLong(uuid16.getMostSignificantBits()
        ).putLong(uuid16.getLeastSignificantBits()
        ).put(rand16
        ).array());
    }

    public static NxId valueOf(String s) throws IllegalArgumentException {
        if (64 != s.length()) {
            throw new IllegalArgumentException();
        }
        byte[] bytes = new byte[32];
        for (int i = 0; i < 64; i += 2) {
            int a = hexToNibble[s.charAt(i)];
            if (a < 0) {
                throw new IllegalArgumentException();
            }
            int b = hexToNibble[s.charAt(i + 1)];
            if (b < 0) {
                throw new IllegalArgumentException();
            }
            bytes[i / 2] = (byte) ((a << 4) | b);
        }
        NxId id = new NxId(bytes);
        assert id.toString().equals(s);
        return id;
    }


    private final byte[] bytes;
    private final int hashCode;


    private NxId(byte[] bytes) {
        if (32 != bytes.length) {
            throw new IllegalArgumentException();
        }
        this.bytes = bytes;
        hashCode = Arrays.hashCode(bytes);
    }


    @Override
    public String toString() {
        CharBuffer cb = CharBuffer.allocate(64);
        for (int i = 0; i < 32; ++i) {
            cb.put(byteToHex[0xFF & bytes[i]]);
        }
        return new String(cb.array());
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NxId)) {
            return false;
        }
        NxId b = (NxId) obj;
        return hashCode == b.hashCode && Arrays.equals(bytes, b.bytes);
    }


    /////// HEX LUTs ///////

    private static final char[] nibbleToHex;
    private static final int[] hexToNibble;
    private static final char[][] byteToHex;
    static {
        nibbleToHex = new char[16];
        for (int i = 0; i < 16; ++i) {
            nibbleToHex[i] = Character.toLowerCase(Integer.toHexString(i).charAt(0));
        }
        hexToNibble = new int[128];
        for (int i = 0, n = hexToNibble.length; i < n; ++i) {
            hexToNibble[i] = -1;
        }
        for (int i = 0; i < 16; ++i) {
            hexToNibble[Character.toLowerCase(nibbleToHex[i])] = i;
            hexToNibble[Character.toUpperCase(nibbleToHex[i])] = i;
        }
        byteToHex = new char[256][];
        for (int i = 0; i < 256; ++i) {
            byteToHex[i] = new char[]{nibbleToHex[(i >>> 4) & 0x0F], nibbleToHex[i & 0x0F]};
        }
    }
}

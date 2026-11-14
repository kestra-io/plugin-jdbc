package io.kestra.plugin.jdbc;

import java.util.Arrays;

public final class IonTestUtils {
    private static final byte[] VERSION_MARKER = { (byte) 0xE0, 0x01, 0x00, (byte) 0xEA };

    private IonTestUtils() {
    }

    public static int countVersionMarkers(byte[] data) {
        var count = 0;
        for (var i = 0; i <= data.length - VERSION_MARKER.length; i++) {
            if (Arrays.equals(data, i, i + VERSION_MARKER.length, VERSION_MARKER, 0, VERSION_MARKER.length)) {
                count++;
            }
        }
        return count;
    }
}

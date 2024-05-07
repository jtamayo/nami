package edu.stanford.nami;

import java.io.ByteArrayOutputStream;

import com.google.common.base.Charsets;
import com.google.common.primitives.Longs;

import lombok.Value;

@Value
public final class VKey {
    long tid;
    String key;

    public byte[] toBytes() {
        // optimize for ASCII strings
        // TODO get rid of all this byte copying
        var baos = new ByteArrayOutputStream(8 + key.length());
        baos.writeBytes(Longs.toByteArray(tid));
        baos.writeBytes(key.getBytes(Charsets.UTF_8));
        return baos.toByteArray();
    }
}

package edu.stanford.nami;

import java.nio.ByteBuffer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;

public class ProtoUtils {

  static ByteString convertToRatisByteString(com.google.protobuf.ByteString googleByteString) {
    ByteBuffer readOnlyByteBuffer = googleByteString.asReadOnlyByteBuffer();
    return ByteString.copyFrom(readOnlyByteBuffer);
  }

  static com.google.protobuf.ByteString convertToGoogleByteString(ByteString ratisByteString) {
    ByteBuffer readOnlyByteBuffer = ratisByteString.asReadOnlyByteBuffer();
    return com.google.protobuf.ByteString.copyFrom(readOnlyByteBuffer);
  }
}

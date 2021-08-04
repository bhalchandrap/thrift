package com.pinterest.commons.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

import java.io.Serializable;

/**
 * Enables partial deserialization of binary-encoded thrift objects.
 *
 * This class is meant to be a helper class for {@link PartialThriftCodec}.
 * It cannot be used separately on its own.
 */
public class PartialThriftBinaryProtocol extends PartialThriftProtocol implements Serializable {

  public PartialThriftBinaryProtocol() {
  }

  @Override
  protected TProtocol createProtocol() {
    return new TBinaryProtocol(transport);
  }

  // -----------------------------------------------------------------
  // Additional methods to improve performance.

  @Override
  public int readFieldBeginData() throws TException {
    byte type = readByte();
    if (type == TType.STOP) {
      return TFieldData.encode(type);
    }

    short id = readI16();
    return TFieldData.encode(type, id);
  }

  // -----------------------------------------------------------------
  // Methods passed through to the wrapped protocol when not skipping.

  @Override
  public short readI16() throws TException {
    if (skipMode) {
      transport.consumeBuffer(2);
      return 0;
    } else {
      return super.readI16();
    }
  }

  @Override
  public long readI64() throws TException {
    if (skipMode) {
      transport.consumeBuffer(8);
      return 0;
    } else {
      return super.readI64();
    }
  }
}

package com.pinterest.commons.thrift;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TType;

import java.io.Serializable;
import java.nio.ByteBuffer;

/**
 * Enables partial deserialization of compact-encoded thrift objects.
 *
 * This class is meant to be a helper class for {@link PartialThriftCodec}.
 * It cannot be used separately on its own.
 */
public class PartialThriftCompactProtocol extends PartialThriftProtocol implements Serializable {

  public PartialThriftCompactProtocol() {
  }

  @Override
  protected TProtocol createProtocol() {
    return new TCompactProtocol(transport);
  }

  // -----------------------------------------------------------------
  // Additional methods to improve performance.

  @Override
  public int readFieldBeginData() throws TException {
    // Having to call readFieldBegin() to compute TFieldData really results in lower
    // performance. However, readFieldBegin() accesses some private vars that this method
    // does not have access to. We could make it more performant when contributing to
    // origianl source code.

    TField tfield = readFieldBegin();
    return TFieldData.encode(tfield.type, tfield.id);
  }

  protected void skipBool() throws TException {
    this.readBool();
  }

  protected void skipI16() throws TException {
    this.readI16();
  }

  protected void skipI32() throws TException {
    this.readI32();
  }

  protected void skipI64() throws TException {
    this.readI64();
  }

  protected void skipBinary() throws TException {
    int size = intToZigZag(readI32());
    this.skipBytes(size);
  }

  // -----------------------------------------------------------------
  // Methods passed through to the wrapped protocol when not skipping.

  @Override
  public String readString() throws TException {
    if (skipMode) {
      int length = readI32();
      length = intToZigZag(length);
      transport.consumeBuffer(length);
      return "";
    } else {
      return tprot.readString();
    }
  }

  @Override
  public ByteBuffer readBinary() throws TException {
    if (skipMode) {
      int length = readI32();
      length = intToZigZag(length);
      transport.consumeBuffer(length);
      return emptyByteBuffer;
    } else {
      return tprot.readBinary();
    }
  }

  private int intToZigZag(int n) {
    return (n << 1) ^ (n >> 31);
  }
}

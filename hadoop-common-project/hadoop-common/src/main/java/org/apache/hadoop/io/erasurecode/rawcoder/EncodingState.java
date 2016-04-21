/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.classification.InterfaceAudience;

import java.nio.ByteBuffer;

/**
 * A utility class that maintains encoding state during an encode call.
 */
@InterfaceAudience.Private
class EncodingState {
  private final RawErasureEncoder encoder;
  private final int encodeLength;
  private final boolean usingDirectBuffer;

  EncodingState(RawErasureEncoder encoder,
                ByteBuffer[] inputs, ByteBuffer[] outputs) {
    this.encoder = encoder;
    ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
    this.encodeLength = validInput.remaining();
    this.usingDirectBuffer = validInput.isDirect();

    checkParameters(inputs, outputs);
    checkBuffers(inputs);
    checkBuffers(outputs);
  }

  EncodingState(RawErasureEncoder encoder,
                byte[][] inputs, byte[][] outputs) {
    this.encoder = encoder;
    byte[] validInput = CoderUtil.findFirstValidInput(inputs);
    this.encodeLength = validInput.length;
    this.usingDirectBuffer = false;

    checkParameters(inputs, outputs);
    checkBuffers(inputs);
    checkBuffers(outputs);
  }

  int getEncodeLength() {
    return encodeLength;
  }

  boolean usingDirectBuffer() {
    return usingDirectBuffer;
  }

  /**
   * Check and validate decoding parameters, throw exception accordingly.
   * @param inputs input buffers to check
   * @param outputs output buffers to check
   */
  <T> void checkParameters(T[] inputs, T[] outputs) {
    if (inputs.length != encoder.getNumDataUnits()) {
      throw new HadoopIllegalArgumentException("Invalid inputs length");
    }
    if (outputs.length != encoder.getNumParityUnits()) {
      throw new HadoopIllegalArgumentException("Invalid outputs length");
    }
  }

  /**
   * Check and ensure the buffers are of the desired length and type, direct
   * buffers or not.
   * @param buffers the buffers to check
   */
  void checkBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.remaining() != encodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, not of length " + encodeLength);
      }
      if (buffer.isDirect() != usingDirectBuffer) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, isDirect should be " + usingDirectBuffer);
      }
    }
  }

  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkBuffers(byte[][] buffers) {
    for (byte[] buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.length != encodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer not of length " + encodeLength);
      }
    }
  }
}

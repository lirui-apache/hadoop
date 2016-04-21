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
 * A utility class that maintains decoding state during a decode call.
 */
@InterfaceAudience.Private
class DecodingState {
  private final RawErasureDecoder decoder;
  private final int decodeLength;
  private final boolean usingDirectBuffer;

  DecodingState(RawErasureDecoder decoder, ByteBuffer[] inputs,
                int[] erasedIndexes, ByteBuffer[] outputs) {
    this.decoder = decoder;
    ByteBuffer validInput = CoderUtil.findFirstValidInput(inputs);
    this.decodeLength = validInput.remaining();
    this.usingDirectBuffer = validInput.isDirect();

    checkParameters(inputs, erasedIndexes, outputs);
    checkInputBuffers(inputs);
    checkOutputBuffers(outputs);
  }

  DecodingState(RawErasureDecoder decoder, byte[][] inputs,
                int[] erasedIndexes, byte[][] outputs) {
    this.decoder = decoder;
    byte[] validInput = CoderUtil.findFirstValidInput(inputs);
    this.decodeLength = validInput.length;
    this.usingDirectBuffer = false;

    checkParameters(inputs, erasedIndexes, outputs);
    checkInputBuffers(inputs);
    checkOutputBuffers(outputs);
  }

  int getDecodeLength() {
    return decodeLength;
  }

  boolean usingDirectBuffer() {
    return usingDirectBuffer;
  }

  /**
   * Check and validate decoding parameters, throw exception accordingly. The
   * checking assumes it's a MDS code. Other code  can override this.
   * @param inputs input buffers to check
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to check
   */
  <T> void checkParameters(T[] inputs, int[] erasedIndexes,
                           T[] outputs) {
    if (inputs.length != decoder.getNumParityUnits() +
        decoder.getNumDataUnits()) {
      throw new IllegalArgumentException("Invalid inputs length");
    }

    if (erasedIndexes.length != outputs.length) {
      throw new HadoopIllegalArgumentException(
          "erasedIndexes and outputs mismatch in length");
    }

    if (erasedIndexes.length > decoder.getNumParityUnits()) {
      throw new HadoopIllegalArgumentException(
          "Too many erased, not recoverable");
    }
  }

  /**
   * Check and ensure the buffers are of the desired length and type, direct
   * buffers or not.
   * @param buffers the buffers to check
   */
  void checkInputBuffers(ByteBuffer[] buffers) {
    int validInputs = 0;

    for (ByteBuffer buffer : buffers) {
      if (buffer == null) {
        continue;
      }

      if (buffer.remaining() != decodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, not of length " + decodeLength);
      }
      if (buffer.isDirect() != usingDirectBuffer) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, isDirect should be " + usingDirectBuffer);
      }

      validInputs++;
    }

    if (validInputs < decoder.getNumDataUnits()) {
      throw new HadoopIllegalArgumentException(
          "No enough valid inputs are provided, not recoverable");
    }
  }

  /**
   * Check and ensure the buffers are of the desired length and type, direct
   * buffers or not.
   * @param buffers the buffers to check
   */
  void checkOutputBuffers(ByteBuffer[] buffers) {
    for (ByteBuffer buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.remaining() != decodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, not of length " + decodeLength);
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
  void checkInputBuffers(byte[][] buffers) {
    int validInputs = 0;

    for (byte[] buffer : buffers) {
      if (buffer == null) {
        continue;
      }

      if (buffer.length != decodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer, not of length " + decodeLength);
      }

      validInputs++;
    }

    if (validInputs < decoder.getNumDataUnits()) {
      throw new HadoopIllegalArgumentException(
          "No enough valid inputs are provided, not recoverable");
    }
  }

  /**
   * Check and ensure the buffers are of the desired length.
   * @param buffers the buffers to check
   */
  void checkOutputBuffers(byte[][] buffers) {
    for (byte[] buffer : buffers) {
      if (buffer == null) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer found, not allowing null");
      }

      if (buffer.length != decodeLength) {
        throw new HadoopIllegalArgumentException(
            "Invalid buffer not of length " + decodeLength);
      }
    }
  }
}

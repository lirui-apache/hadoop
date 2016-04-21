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

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.io.erasurecode.ECChunk;

import java.nio.ByteBuffer;

/**
 * An abstract raw erasure encoder that's to be inherited by new encoders.
 *
 * Raw erasure coder is part of erasure codec framework, where erasure coder is
 * used to encode/decode a group of blocks (BlockGroup) according to the codec
 * specific BlockGroup layout and logic. An erasure coder extracts chunks of
 * data from the blocks and can employ various low level raw erasure coders to
 * perform encoding/decoding against the chunks.
 *
 * To distinguish from erasure coder, here raw erasure coder is used to mean the
 * low level constructs, since it only takes care of the math calculation with
 * a group of byte buffers.
 *
 * Note it mainly provides encode() calls, which should be stateless and may be
 * made thread-safe in future.
 */
@InterfaceAudience.Private
public abstract class RawErasureEncoder {

  private final ErasureCoderOptions coderOptions;

  public RawErasureEncoder(ErasureCoderOptions coderOptions) {
    this.coderOptions = coderOptions;
  }

  /**
   * Encode with inputs and generates outputs.
   *
   * Note, for both inputs and outputs, no mixing of on-heap buffers and direct
   * buffers are allowed.
   *
   * If the coder option ALLOW_CHANGE_INPUTS is set true (false by default), the
   * content of input buffers may change after the call, subject to concrete
   * implementation. Anyway the positions of input buffers will move forward.
   *
   * @param inputs input buffers to read data from. The buffers' remaining will
   *               be 0 after encoding
   * @param outputs output buffers to put the encoded data into, ready to read
   *                after the call
   */
  public void encode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    EncodingState encodingState = new EncodingState(this, inputs, outputs);

    boolean usingDirectBuffer = encodingState.usingDirectBuffer();
    int dataLen = encodingState.getEncodeLength();
    if (dataLen == 0) {
      return;
    }

    int[] inputPositions = new int[inputs.length];
    for (int i = 0; i < inputPositions.length; i++) {
      if (inputs[i] != null) {
        inputPositions[i] = inputs[i].position();
      }
    }

    if (usingDirectBuffer) {
      doEncode(encodingState, inputs, outputs);
    } else {
      int[] inputOffsets = new int[inputs.length];
      int[] outputOffsets = new int[outputs.length];
      byte[][] newInputs = new byte[inputs.length][];
      byte[][] newOutputs = new byte[outputs.length][];

      ByteBuffer buffer;
      for (int i = 0; i < inputs.length; ++i) {
        buffer = inputs[i];
        inputOffsets[i] = buffer.arrayOffset() + buffer.position();
        newInputs[i] = buffer.array();
      }

      for (int i = 0; i < outputs.length; ++i) {
        buffer = outputs[i];
        outputOffsets[i] = buffer.arrayOffset() + buffer.position();
        newOutputs[i] = buffer.array();
      }

      doEncode(encodingState, newInputs,
          inputOffsets, newOutputs, outputOffsets);
    }

    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        // dataLen bytes consumed
        inputs[i].position(inputPositions[i] + dataLen);
      }
    }
  }

  /**
   * Perform the real encoding work using direct ByteBuffer.
   * @param encodingState the encoder state
   * @param inputs Direct ByteBuffers expected
   * @param outputs Direct ByteBuffers expected
   */
  protected abstract void doEncode(EncodingState encodingState,
                                   ByteBuffer[] inputs, ByteBuffer[] outputs);

  /**
   * Encode with inputs and generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param outputs output buffers to put the encoded data into, read to read
   *                after the call
   */
  public void encode(byte[][] inputs, byte[][] outputs) {
    EncodingState encodingState = new EncodingState(this, inputs, outputs);

    int dataLen = encodingState.getEncodeLength();
    if (dataLen == 0) {
      return;
    }

    int[] inputOffsets = new int[inputs.length]; // ALL ZERO
    int[] outputOffsets = new int[outputs.length]; // ALL ZERO

    doEncode(encodingState, inputs, inputOffsets, outputs, outputOffsets);
  }

  /**
   * Perform the real encoding work using bytes array, supporting offsets
   * and lengths.
   * @param encodingState the encoder state
   * @param inputs the input byte arrays to read data from
   * @param inputOffsets offsets for the input byte arrays to read data from
   * @param outputs the output byte arrays to write resultant data into
   * @param outputOffsets offsets from which to write resultant data into
   */
  protected abstract void doEncode(EncodingState encodingState, byte[][] inputs,
                                   int[] inputOffsets, byte[][] outputs,
                                   int[] outputOffsets);

  /**
   * Encode with inputs and generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param outputs output buffers to put the encoded data into, read to read
   *                after the call
   */
  public void encode(ECChunk[] inputs, ECChunk[] outputs) {
    ByteBuffer[] newInputs = ECChunk.toBuffers(inputs);
    ByteBuffer[] newOutputs = ECChunk.toBuffers(outputs);
    encode(newInputs, newOutputs);
  }

  public int getNumDataUnits() {
    return coderOptions.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return coderOptions.getNumParityUnits();
  }

  public int getNumAllUnits() {
    return coderOptions.getNumAllUnits();
  }

  /**
   * Tell if direct buffer is preferred or not. It's for callers to
   * decide how to allocate coding chunk buffers, using DirectByteBuffer or
   * bytes array. It will return false by default.
   * @return true if native buffer is preferred for performance consideration,
   * otherwise false.
   */
  public boolean preferDirectBuffer() {
    return false;
  }

  /**
   * Allow change into input buffers or not while perform encoding/decoding.
   * @return true if it's allowed to change inputs, false otherwise
   */
  public boolean allowChangeInputs() {
    return coderOptions.allowChangeInputs();
  }

  /**
   * Allow to dump verbose info during encoding/decoding.
   * @return true if it's allowed to do verbose dump, false otherwise.
   */
  public boolean allowVerboseDump() {
    return coderOptions.allowVerboseDump();
  }

  /**
   * Should be called when release this coder. Good chance to release encoding
   * or decoding buffers
   */
  public void release() {
    // Nothing to do here.
  }
}

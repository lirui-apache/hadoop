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
 * An abstract raw erasure decoder that's to be inherited by new decoders.
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
 * Note it mainly provides decode() calls, which should be stateless and may be
 * made thread-safe in future.
 */
@InterfaceAudience.Private
public abstract class RawErasureDecoder {

  private final ErasureCoderOptions coderOptions;

  public RawErasureDecoder(ErasureCoderOptions coderOptions) {
    this.coderOptions = coderOptions;
  }

  /**
   * Decode with inputs and erasedIndexes, generates outputs.
   * How to prepare for inputs:
   * 1. Create an array containing data units + parity units. Please note the
   *    data units should be first or before the parity units.
   * 2. Set null in the array locations specified via erasedIndexes to indicate
   *    they're erased and no data are to read from;
   * 3. Set null in the array locations for extra redundant items, as they're
   *    not necessary to read when decoding. For example in RS-6-3, if only 1
   *    unit is really erased, then we have 2 extra items as redundant. They can
   *    be set as null to indicate no data will be used from them.
   *
   * For an example using RS (6, 3), assuming sources (d0, d1, d2, d3, d4, d5)
   * and parities (p0, p1, p2), d2 being erased. We can and may want to use only
   * 6 units like (d1, d3, d4, d5, p0, p2) to recover d2. We will have:
   *     inputs = [null(d0), d1, null(d2), d3, d4, d5, p0, null(p1), p2]
   *     erasedIndexes = [2] // index of d2 into inputs array
   *     outputs = [a-writable-buffer]
   *
   * Note, for both inputs and outputs, no mixing of on-heap buffers and direct
   * buffers are allowed.
   *
   * If the coder option ALLOW_CHANGE_INPUTS is set true (false by default), the
   * content of input buffers may change after the call, subject to concrete
   * implementation.
   *
   * @param inputs input buffers to read data from. The buffers' remaining will
   *               be 0 after decoding
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   */
  public void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                     ByteBuffer[] outputs) {
    DecodingState decodingState = new DecodingState(this, inputs,
        erasedIndexes, outputs);

    boolean usingDirectBuffer = decodingState.usingDirectBuffer();
    int dataLen = decodingState.getDecodeLength();
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
      doDecode(decodingState, inputs, erasedIndexes, outputs);
    } else {
      int[] inputOffsets = new int[inputs.length];
      int[] outputOffsets = new int[outputs.length];
      byte[][] newInputs = new byte[inputs.length][];
      byte[][] newOutputs = new byte[outputs.length][];

      ByteBuffer buffer;
      for (int i = 0; i < inputs.length; ++i) {
        buffer = inputs[i];
        if (buffer != null) {
          inputOffsets[i] = buffer.arrayOffset() + buffer.position();
          newInputs[i] = buffer.array();
        }
      }

      for (int i = 0; i < outputs.length; ++i) {
        buffer = outputs[i];
        outputOffsets[i] = buffer.arrayOffset() + buffer.position();
        newOutputs[i] = buffer.array();
      }

      doDecode(decodingState, newInputs, inputOffsets, erasedIndexes,
          newOutputs, outputOffsets);
    }

    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        // dataLen bytes consumed
        inputs[i].position(inputPositions[i] + dataLen);
      }
    }
  }

  /**
   * Perform the real decoding using Direct ByteBuffer.
   * @param decodingState the decoder state
   * @param inputs Direct ByteBuffers expected
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs Direct ByteBuffers expected
   */
  protected abstract void doDecode(DecodingState decodingState,
                                   ByteBuffer[] inputs, int[] erasedIndexes,
                                   ByteBuffer[] outputs);

  /**
   * Decode with inputs and erasedIndexes, generates outputs. More see above.
   *
   * @param inputs input buffers to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   */
  public void decode(byte[][] inputs, int[] erasedIndexes, byte[][] outputs) {
    DecodingState decodingState = new DecodingState(this, inputs,
        erasedIndexes, outputs);

    if (decodingState.getDecodeLength() == 0) {
      return;
    }

    int[] inputOffsets = new int[inputs.length]; // ALL ZERO
    int[] outputOffsets = new int[outputs.length]; // ALL ZERO

    doDecode(decodingState, inputs, inputOffsets, erasedIndexes, outputs,
        outputOffsets);
  }

  /**
   * Perform the real decoding using bytes array, supporting offsets and
   * lengths.
   * @param inputs the input byte arrays to read data from
   * @param inputOffsets offsets for the input byte arrays to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs the output byte arrays to write resultant data into
   * @param outputOffsets offsets from which to write resultant data into
   */
  protected abstract void doDecode(DecodingState decodingState, byte[][] inputs,
                                   int[] inputOffsets, int[] erasedIndexes,
                                   byte[][] outputs, int[] outputOffsets);

  /**
   * Decode with inputs and erasedIndexes, generates outputs. More see above.
   *
   * Note, for both input and output ECChunks, no mixing of on-heap buffers and
   * direct buffers are allowed.
   *
   * @param inputs input buffers to read data from
   * @param erasedIndexes indexes of erased units in the inputs array
   * @param outputs output buffers to put decoded data into according to
   *                erasedIndexes, ready for read after the call
   */
  public void decode(ECChunk[] inputs, int[] erasedIndexes,
                     ECChunk[] outputs) {
    ByteBuffer[] newInputs = CoderUtil.toBuffers(inputs);
    ByteBuffer[] newOutputs = CoderUtil.toBuffers(outputs);
    decode(newInputs, erasedIndexes, newOutputs);
  }

  public int getNumDataUnits() {
    return coderOptions.getNumDataUnits();
  }

  public int getNumParityUnits() {
    return coderOptions.getNumParityUnits();
  }

  protected int getNumAllUnits() {
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

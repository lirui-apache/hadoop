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

import java.nio.ByteBuffer;

/**
 * A dummy encoder that doesn't really encode the data
 */
@InterfaceAudience.Private
public class DummyRawEncoder extends AbstractRawErasureEncoder {
  public DummyRawEncoder(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
  }

  @Override
  protected void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    // Nothing to do. Output buffers have already been reset
  }

  @Override
  protected void doEncode(byte[][] inputs, int[] inputOffsets, int dataLen,
      byte[][] outputs, int[] outputOffsets) {
    // Nothing to do. Output buffers have already been reset
  }
}

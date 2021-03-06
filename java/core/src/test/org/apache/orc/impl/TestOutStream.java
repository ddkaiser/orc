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

package org.apache.orc.impl;

import org.apache.orc.CompressionCodec;
import org.apache.orc.PhysicalWriter;
import org.junit.Test;
import org.mockito.Mockito;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

public class TestOutStream {

  @Test
  public void testFlush() throws Exception {
    PhysicalWriter.OutputReceiver receiver =
        Mockito.mock(PhysicalWriter.OutputReceiver.class);
    CompressionCodec codec = new ZlibCodec();
    OutStream stream = new OutStream("test", 128*1024, codec, receiver);
    assertEquals(0L, stream.getBufferSize());
    stream.write(new byte[]{0, 1, 2});
    stream.flush();
    Mockito.verify(receiver).output(Mockito.any(ByteBuffer.class));
    assertEquals(0L, stream.getBufferSize());
  }
}

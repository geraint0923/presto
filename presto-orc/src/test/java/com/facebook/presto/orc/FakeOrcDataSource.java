/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.orc;

import io.airlift.slice.FixedLengthSliceInput;

import java.io.IOException;
import java.util.Map;

public class FakeOrcDataSource
        implements OrcDataSource
{
    public static final FakeOrcDataSource INSTANCE = new FakeOrcDataSource();

    private FakeOrcDataSource() {}

    @Override
    public long getReadBytes()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getReadTimeNanos()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSize()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void readFully(long position, byte[] buffer)
            throws IOException
    {
        // do nothing
    }

    @Override
    public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
            throws IOException
    {
        // do nothing
    }

    @Override
    public <K> Map<K, FixedLengthSliceInput> readFully(Map<K, DiskRange> diskRanges)
            throws IOException
    {
        throw new UnsupportedOperationException();
    }
}

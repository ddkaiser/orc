/*
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

package org.apache.orc.impl.writer;

import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.ogc.OGCGeometry;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.OrcProto.SpatialColumnEncoding.Kind;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;

public class GeometryTreeWriter extends TreeWriterBase {
    private final IntegerWriter writerLength;
    private final TreeWriter valueWriter;
    private boolean isDirectV2 = true;
    Kind spatialEncoding = null;
    List<OGCGeometry> geometries = new ArrayList<OGCGeometry>();
    Envelope2D envelope = new Envelope2D();

    GeometryTreeWriter(int columnId, TypeDescription schema, WriterContext writer, boolean nullable) throws IOException {
        super(columnId, schema, writer, nullable);
        TypeDescription.Category category = schema.getCategory();
        if (category == TypeDescription.Category.POINT) {
            // empty if block
        }
        this.spatialEncoding = Kind.WKT;
        switch (spatialEncoding) {
            case WKB: 
            case Shape: {
                this.valueWriter = new BinaryTreeWriter(columnId, schema, writer, nullable);
                break;
            }
            default: {
                this.valueWriter = new StringTreeWriter(columnId, schema, writer, nullable);
            }
        }
        this.isDirectV2 = this.isNewWriteFormat(writer);
        this.writerLength = this.createIntegerWriter((PositionedOutputStream)writer.createStream(this.id, OrcProto.Stream.Kind.LENGTH), false, this.isDirectV2, writer);
        if (this.rowIndexPosition != null) {
            this.recordPosition((PositionRecorder)this.rowIndexPosition);
        }
    }

    OrcProto.ColumnEncoding.Builder getEncoding() {
        OrcProto.ColumnEncoding.Builder result = super.getEncoding();
        if (this.isDirectV2) {
            result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT_V2);
        } else {
            result.setKind(OrcProto.ColumnEncoding.Kind.DIRECT);
        }
        return result;
    }

    public void createRowIndexEntry() throws IOException {
        super.createRowIndexEntry();
        this.valueWriter.createRowIndexEntry();
    }

    public void writeBatch(ColumnVector vector, int offset, int length) throws IOException {
        super.writeBatch(vector, offset, length);
        BytesColumnVector vec = (BytesColumnVector)vector;
        String geometryString = null;
        OGCGeometry geometry = null;
        int currentOffset = 0;
        int currentLength = 0;
        for (int i = 0; i < length; ++i) {
            if (vec.isNull[i + offset]) continue;
            int nextLength = vec.length[offset + i];
            int nextOffset = vec.start[offset + i];
            this.writerLength.write((long)nextLength);
            if (currentLength == 0) {
                currentOffset = nextOffset;
                currentLength = nextLength;
            } else if (currentOffset + currentLength != nextOffset) {
                this.valueWriter.writeBatch(vector, currentOffset, currentLength);
                currentOffset = nextOffset;
                currentLength = nextLength;
            } else {
                currentLength += nextLength;
            }
            if (this.createBloomFilter) {
                if (this.bloomFilter != null) {
                    this.bloomFilter.addLong((long)nextLength);
                }
                this.bloomFilterUtf8.addLong((long)nextLength);
            }
            if (!this.createSpatialIndex) continue;
            Envelope2D env = null;
            for (int j = 0; j < currentLength; ++j) {
                switch (spatialEncoding) {
                    case WKB: {
                    		ByteBuffer buffer = ByteBuffer.wrap(vec.vector[j + currentOffset]);
                        geometry = OGCGeometry.fromBinary(buffer);
                        break;
                    }
                    case Shape: {
                  		ByteBuffer buffer = ByteBuffer.wrap(vec.vector[j + currentOffset]);
                        geometry = OGCGeometry.fromEsriShape(buffer);
                        break;
                    }
                    case WKT: {
                        geometryString = new String(vec.vector[j + currentOffset], "UTF8");
                        geometry = OGCGeometry.fromText((String)geometryString);
                        break;
                    }
                    case GeoJson: {
                        geometryString = new String(vec.vector[j + currentOffset], "UTF8");
                        geometry = OGCGeometry.fromGeoJson((String)geometryString);
                        break;
                    }
                    default: {
                        geometry = null;
                    }
                }
                if (geometry == null) continue;
                this.geometries.add(geometry);
                env = new Envelope2D();
                geometry.getEsriGeometry().queryEnvelope2D(env);
                this.envelope.merge(env);
            }
        }
        if (currentLength != 0) {
            this.valueWriter.writeBatch(vector, currentOffset, currentLength);
        }
    }

    public void writeStripe(OrcProto.StripeFooter.Builder builder, OrcProto.StripeStatistics.Builder stats, int requiredIndexEntries) throws IOException {
        super.writeStripe(builder, stats, requiredIndexEntries);
        this.writerLength.flush();
        this.valueWriter.writeStripe(builder, stats, requiredIndexEntries);
        if (this.rowIndexPosition != null) {
            this.recordPosition((PositionRecorder)this.rowIndexPosition);
        }
        if (this.createSpatialIndex) {
        	
            int depth = (int)Math.ceil(Math.pow(this.geometries.size(), 0.25));
            Geometry geom = null;
            Envelope2D env = null;
            int val = 0;
            QuadTree tree = new QuadTree(this.envelope, depth);
            for (int i = 0; i < this.geometries.size(); ++i) {
                geom = this.geometries.get(i).getEsriGeometry();
                geom.queryLooseEnvelope2D(env);
                val = tree.insert(i, env);
            }
        }
    }

    void recordPosition(PositionRecorder recorder) throws IOException {
        super.recordPosition(recorder);
        this.writerLength.getPosition(recorder);
    }

    public void updateFileStatistics(OrcProto.StripeStatistics stats) {
        super.updateFileStatistics(stats);
        this.valueWriter.updateFileStatistics(stats);
    }

    public long estimateMemory() {
        return super.estimateMemory() + this.writerLength.estimateMemory() + this.valueWriter.estimateMemory();
    }

    public long getRawDataSize() {
        return this.valueWriter.getRawDataSize();
    }

    public void writeFileStatistics(OrcProto.Footer.Builder footer) {
        super.writeFileStatistics(footer);
        this.valueWriter.writeFileStatistics(footer);
    }
}
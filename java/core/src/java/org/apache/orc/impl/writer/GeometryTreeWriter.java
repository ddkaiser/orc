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

import com.esri.core.geometry.Envelope;
import com.esri.core.geometry.Envelope2D;
import com.esri.core.geometry.Geometry;
import com.esri.core.geometry.Point2D;
import com.esri.core.geometry.QuadTree;
import com.esri.core.geometry.ogc.OGCGeometry;
import com.esri.core.geometry.ogc.OGCPoint;
import com.esri.core.geometry.ogc.OGCPolygon;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.util.JavaDataModel;
import org.apache.lucene.spatial.util.MortonEncoder;
import org.apache.orc.OrcProto;
import org.apache.orc.TypeDescription;
import org.apache.orc.TypeDescription.Category;
import org.apache.orc.OrcProto.GeometryValueEncodingKind;
import org.apache.orc.SpatialColumnStatistics;
import org.apache.orc.impl.IntegerWriter;
import org.apache.orc.impl.PositionRecorder;
import org.apache.orc.impl.PositionedOutputStream;


public class GeometryTreeWriter extends StringBaseTreeWriter {

    private PositionedOutputStream binaryOutStream = null;
    private IntegerWriter binaryLengthStream = null;
//    private IntegerWriter spatialIndexStream = null;
    private boolean isDirectV2 = true;
    TypeDescription.Category category = null;

    // for spatial support
    
    // since we are subclass of a String writer, default is a string encoding
    // (we have to assign this enum in class signature rather than constructor)
    // 
    GeometryValueEncodingKind spatialEncoding = null;
    List<OGCGeometry> geometries = new ArrayList<OGCGeometry>();
    Envelope2D envelope = new Envelope2D();

    static final class GeometryDecoder {
    	static public OGCGeometry parseGeometry(byte[] val, GeometryValueEncodingKind encoding) {
    		OGCGeometry geometry = null;
	        switch (encoding) {                		
		        case WKB: {
		          geometry = OGCGeometry.fromBinary(ByteBuffer.wrap(val));
		          break;
		        }
		        case Shape: {
		          geometry = OGCGeometry.fromEsriShape(ByteBuffer.wrap(val));
		          break;
		        }
		        case WKT: {
		          geometry = OGCGeometry.fromText(new String(val, StandardCharsets.UTF_8));
		          break;
		        }
		        case GeoJson: {
		          geometry = OGCGeometry.fromGeoJson(new String(val, StandardCharsets.UTF_8));
		          break;
		        }
		        default: {
		        	//TODO: exception, or LOG?
		          geometry = null;
		        }
	        }
	        return(geometry);
	    }
    	static public boolean isStringEncoding(GeometryValueEncodingKind encoding) {
	        switch (encoding) {                		
			        case WKB:
			        case Shape: {
			          return false;
			        }
			        
			        case WKT:
			        case GML:
			        case KML:
			        case GeoJson: {
			          return true;
			        }
			        
			        default: {
			        	//TODO: exception, or LOG?
			          return false;
			        }
	        }
    	}
    	static public boolean isBinaryEncoding(GeometryValueEncodingKind encoding) {
	        switch (encoding) {                		
			        case WKB:
			        case Shape: {
			          return true;
			        }
			        
			        case WKT:
			        case GML:
			        case KML:
			        case GeoJson: {
			          return false;
			        }
			        
			        default: {
			        	//TODO: exception, or LOG?
			          return false;
			        }
	        }
    	}
    }
    
    GeometryTreeWriter(int columnId, TypeDescription schema, WriterContext writer, boolean nullable) throws IOException {

    	super(columnId, schema, writer, nullable);
        this.isDirectV2 = this.isNewWriteFormat(writer);
        this.spatialEncoding = schema.getGeomEncoding();
        
        // if this column is storing binary encoded Geometry,
        //   use binaryOutStream and binaryLengthStream
        //   if not, set streams to null, and we default to parent (StreamBaseTreeWriter)
      	binaryOutStream = (GeometryDecoder.isBinaryEncoding(spatialEncoding)) 
      		? writer.createStream(columnId, OrcProto.Stream.Kind.DATA) : null;

      	binaryLengthStream = (GeometryDecoder.isBinaryEncoding(spatialEncoding))
  			? createIntegerWriter(writer.createStream(id,
  	    		OrcProto.Stream.Kind.LENGTH), false, isDirectV2, writer) : null;

        category = schema.getCategory();

//        spatialIndexStream = createIntegerWriter(writer.createStream(id,
//            OrcProto.Stream.Kind.SECONDARY), true, isDirectV2, writer);
        
        if (this.rowIndexPosition != null) {
            this.recordPosition((PositionRecorder)this.rowIndexPosition);
        }
        
        // TODO: select impl using either linear tree format
        // or: research, would a Hilbert R-Tree support both Point&Poly 
        // with the tradeoff of avoiding linearized key for storage?
        // could be that an *internal* index stream would work fine
        // or does Hilbert R-Tree convert to linear key somehow?
        // https://en.wikipedia.org/wiki/Quadtree
        // https://en.wikipedia.org/wiki/Hilbert_R-tree
        
        if (category == TypeDescription.Category.POINT) {
            // instantiate a PR (Point Region) Quadtree
        } else if ((category == TypeDescription.Category.POLYLINE)
        		|| (category == TypeDescription.Category.POLYGON)) {
        	// instantiate a PM3 or PM2 (Polygonal Map) Quadtree 
        }
    }

    @Override
    public void createRowIndexEntry() throws IOException {
        if (GeometryDecoder.isStringEncoding(spatialEncoding)) {
        	super.createRowIndexEntry();
        } else if (GeometryDecoder.isBinaryEncoding(spatialEncoding)) {
        	//TODO: implement local for binary
        }
//        this.valueWriter.createRowIndexEntry();
    }

    @Override
    public void writeBatch(ColumnVector vector, int offset, int length) throws IOException {
    	OGCGeometry geometry;

    	super.writeBatch(vector, offset, length);
      
    	if (GeometryDecoder.isBinaryEncoding(this.spatialEncoding)) {
    		writeBatchBinary(vector, offset, length);
    	} else if (GeometryDecoder.isStringEncoding(this.spatialEncoding)) {
    		writeBatchString(vector, offset, length);
    	}
      
    	if (this.createSpatialIndex) {
    		BytesColumnVector vec = (BytesColumnVector) vector;
	    	for (int j = 0; j < length; ++j) {
		      	geometry = GeometryDecoder.parseGeometry(vec.vector[j + offset], spatialEncoding);
		          if (geometry == null) continue;
		          this.geometries.add(geometry);
		          envelope = new Envelope2D();
		          geometry.getEsriGeometry().queryEnvelope2D(envelope);
		//                geometry.getEsriGeometry().queryLooseEnvelope2D(envelope);
		          // queryLooseEnvelope appears to be same as regular queryEnvelope
		          this.envelope.merge(envelope);
	    	}
    	}
    }
    
    public void writeBatchBinary(ColumnVector vector, int offset, int length) throws IOException {
//      super.writeBatch is handled in calling function (writeBatch)
      BytesColumnVector vec = (BytesColumnVector) vector;
      if (vector.isRepeating) {
        if (vector.noNulls || !vector.isNull[0]) {
          for (int i = 0; i < length; ++i) {
            this.binaryOutStream.write(vec.vector[0], vec.start[0],
                vec.length[0]);
            this.binaryLengthStream.write(vec.length[0]);
          }
    			writeSpatialStats(vec.vector[0]);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              bloomFilter.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
            }
            bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
          }
        }
      } else {
        for (int i = 0; i < length; ++i) {
          if (vec.noNulls || !vec.isNull[i + offset]) {
            this.binaryOutStream.write(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
            this.binaryLengthStream.write(vec.length[offset + i]);
      			writeSpatialStats(vec.vector[offset + i]);
            if (createBloomFilter) {
              if (bloomFilter != null) {
                bloomFilter.addBytes(vec.vector[offset + i],
                    vec.start[offset + i], vec.length[offset + i]);
              }
              bloomFilterUtf8.addBytes(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i]);
            }
          }
        }
      }
    }

    public void writeBatchString(ColumnVector vector, int offset, int length) throws IOException {
//      super.writeBatch is handled in calling function (writeBatch)
        BytesColumnVector vec = (BytesColumnVector) vector;
        if (vector.isRepeating) {
        	if (vector.noNulls || !vector.isNull[0]) {
        		if (useDictionaryEncoding) {
        			int id = dictionary.add(vec.vector[0], vec.start[0], vec.length[0]);
        			for (int i = 0; i < length; ++i) {
        				rows.add(id);
        			}
        		} else {
        			for (int i = 0; i < length; ++i) {
        				this.directStreamOutput.write(vec.vector[0], vec.start[0], vec.length[0]);
        				this.lengthOutput.write(vec.length[0]);
        			}
        		}
      			writeSpatialStats(vec.vector[0]);
      			if (createBloomFilter) {
        			if (bloomFilter != null) {
        				// translate from UTF-8 to the default charset
        				bloomFilter.addString(new String(vec.vector[0], vec.start[0],
        						vec.length[0], StandardCharsets.UTF_8));
        			}
        			bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
        		}
        	}
        } else {
        	for (int i = 0; i < length; ++i) {
        		if (vec.noNulls || !vec.isNull[i + offset]) {
        			if (useDictionaryEncoding) {
        				rows.add(dictionary.add(vec.vector[offset + i],
        						vec.start[offset + i], vec.length[offset + i]));
        			} else {
        				this.directStreamOutput.write(vec.vector[offset + i],
        						vec.start[offset + i], vec.length[offset + i]);
        				this.lengthOutput.write(vec.length[offset + i]);
        			}
        			writeSpatialStats(vec.vector[offset + i]);
        			if (createBloomFilter) {
        				if (bloomFilter != null) {
        					// translate from UTF-8 to the default charset
        					bloomFilter.addString(new String(vec.vector[offset + i],
        							vec.start[offset + i], vec.length[offset + i],
        							StandardCharsets.UTF_8));
        				}
        				bloomFilterUtf8.addBytes(vec.vector[offset + i],
        						vec.start[offset + i], vec.length[offset + i]);
        			}
        		}
        	}
        }
    }
    
    public void writeSpatialStats(byte[] encodedGeometry) {
  		OGCGeometry geom = GeometryDecoder.parseGeometry(encodedGeometry, spatialEncoding);
  		switch (category) {
  			case POINT: {
  				OGCPoint point = (OGCPoint) geom;
    			indexStatistics.updateSpatial1D(
    					MortonEncoder.encode(point.X(), point.Y()),
    					MortonEncoder.encodeCeil(point.X(), point.Y()), encodedGeometry.length);
    			break;
  			}
  			case POLYLINE:
  			case POLYGON: {
  				Envelope2D env = new Envelope2D();
  				geom.getEsriGeometry().queryEnvelope2D(env);
		  		Point2D ll = env.getLowerLeft();
		  		Point2D ur = env.getUpperRight();
    		  indexStatistics.updateSpatial2D(
    		  		MortonEncoder.encode(ll.x, ll.y), MortonEncoder.encodeCeil(ur.x,	ur.y),
    		  		encodedGeometry.length);
    		  break;
  			}
  			default:
  				break;
  		}
    }
    
    public void writeStripe(OrcProto.StripeFooter.Builder builder, OrcProto.StripeStatistics.Builder stats, int requiredIndexEntries) throws IOException {
    	super.writeStripe(builder, stats, requiredIndexEntries);
        this.lengthOutput.flush();
//        this.valueWriter.writeStripe(builder, stats, requiredIndexEntries);
        if (this.rowIndexPosition != null) {
            this.recordPosition((PositionRecorder)this.rowIndexPosition);
        }
        
        if (this.createSpatialIndex) {
        			//TODO: avoid esri QuadTree and replace with real hilbert or morton friendly tree
//            int depth = (int)Math.ceil(Math.pow(this.geometries.size(), 0.25));
//            Geometry geom = null;
//            Envelope2D env = null;
//            int val = 0;
//            QuadTree tree = new QuadTree(this.envelope, depth);
//            for (int i = 0; i < this.geometries.size(); ++i) {
//                geom = this.geometries.get(i).getEsriGeometry();
//                geom.queryLooseEnvelope2D(env);
//                val = tree.insert(i, env);
//            }
        }
    }

    @Override
    void recordPosition(PositionRecorder recorder) throws IOException {
        super.recordPosition(recorder);
		// these typing fields aren't init'd yet when superclass constructor runs
		// so we have to check for if not null
        if (spatialEncoding != null) {
	        if (GeometryDecoder.isStringEncoding(spatialEncoding)) {        	
	        	directStreamOutput.getPosition(recorder);
	        	lengthOutput.getPosition(recorder);
	        } else if (GeometryDecoder.isBinaryEncoding(spatialEncoding)) {
	        	if ((binaryOutStream != null) && (binaryLengthStream != null)) {
	        		binaryOutStream.getPosition(recorder);
	        		binaryLengthStream.getPosition(recorder);
	        	}
	        }
        }
    }

    public void updateFileStatistics(OrcProto.StripeStatistics stats) {
        super.updateFileStatistics(stats);
//    	if (GeometryDecoder.isStringGeometry(spatialEncoding)) {
//            super.updateFileStatistics(stats);
//    	} else if (GeometryDecoder.isBinaryGeometry(spatialEncoding)) {
//    		
//    	}
    }

    public void writeFileStatistics(OrcProto.Footer.Builder footer) {
        super.writeFileStatistics(footer);
//    	if (GeometryDecoder.isStringGeometry(spatialEncoding)) {
//            super.writeFileStatistics(footer);
//    	} else if (GeometryDecoder.isBinaryGeometry(spatialEncoding)) {
//    		
//    	}
    }

    @Override
    public long estimateMemory() {
    	long memory = 0L;
        if (GeometryDecoder.isStringEncoding(spatialEncoding)) {
        	memory = super.estimateMemory();
//	        memory = super.estimateMemory() +
//	        		this.directStreamOutput.getBufferSize() +
//	        		this.lengthOutput.estimateMemory();
        } else if (GeometryDecoder.isBinaryEncoding(spatialEncoding)) {
        	memory = this.binaryOutStream.getBufferSize() +
        			this.binaryLengthStream.estimateMemory();
//        	memory = super.estimateMemory() +
//        			this.binaryOutStream.getBufferSize() +
//        			this.binaryLengthStream.estimateMemory();
        }
        return memory;
    }

    @Override
    public long getRawDataSize() {
    	long rawDataSize =0L;
    	if (GeometryDecoder.isStringEncoding(spatialEncoding)) {
    		//rawDataSize = super.getRawDataSize(); // cannot use StringStatisticsImpl default
	        SpatialColumnStatistics scs = (SpatialColumnStatistics) fileStatistics;
	        long numVals = fileStatistics.getNumberOfValues();
	        if (numVals == 0) {
	            return 0;
	        } else {
	          int avgSize = (int) (scs.getSum() / numVals);
	        }
    	} else if (GeometryDecoder.isBinaryEncoding(spatialEncoding)) {
    		
    	}
    	return rawDataSize;
    }
}
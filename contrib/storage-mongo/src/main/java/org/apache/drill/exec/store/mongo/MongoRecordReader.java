/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.mongo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ExecConstants;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.bson.BsonRecordReader;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.complex.fn.JsonReader;
import org.apache.drill.exec.vector.complex.impl.VectorContainerWriter;
import org.apache.drill.shaded.guava.com.google.common.base.Stopwatch;
import org.apache.drill.shaded.guava.com.google.common.collect.Lists;
import org.apache.drill.shaded.guava.com.google.common.collect.Sets;
import org.bson.BsonDocument;
import org.bson.BsonDocumentReader;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.CountOptions;

public class MongoRecordReader extends AbstractRecordReader {
  private static final String COUNT = "count";
  private static final String COUNT_2 = "COUNT";


private static final Logger logger = LoggerFactory.getLogger(MongoRecordReader.class);

  private MongoCollection<BsonDocument> collection;
  private MongoCursor<BsonDocument> cursor;
  private MongoDatabase db;

  private JsonReader jsonReader;
  private BsonRecordReader bsonReader;
  private VectorContainerWriter writer;

  private Document filters;
  private List<Bson> operations;
  private final Document fields;

  private final FragmentContext fragmentContext;

  private final MongoStoragePlugin plugin;

  private final boolean enableAllTextMode;
  private final boolean enableNanInf;
  private final boolean readNumbersAsDouble;
  private boolean unionEnabled;
  private final boolean isBsonRecordReader;

  public MongoRecordReader(BaseMongoSubScanSpec subScanSpec, List<SchemaPath> projectedColumns,
      FragmentContext context, MongoStoragePlugin plugin) {

    fields = new Document();
    // exclude _id field, if not mentioned by user.
    fields.put(DrillMongoConstants.ID, 0);
    setColumns(projectedColumns);
    fragmentContext = context;
    this.plugin = plugin;
    filters = new Document();
    if (subScanSpec instanceof MongoSubScan.MongoSubScanSpec) {
      operations = ((MongoSubScan.MongoSubScanSpec) subScanSpec).getOperations().stream()
        .map(BsonDocument::parse)
        .collect(Collectors.toList());
    } else {
      MongoSubScan.ShardedMongoSubScanSpec shardedMongoSubScanSpec = (MongoSubScan.ShardedMongoSubScanSpec) subScanSpec;
      Map<String, List<Document>> mergedFilters = MongoUtils.mergeFilters(
          shardedMongoSubScanSpec.getMinFilters(), shardedMongoSubScanSpec.getMaxFilters());

      Document pushdownFilters = Optional.ofNullable(shardedMongoSubScanSpec.getFilter())
        .map(Document::parse)
        .orElse(null);
      buildFilters(pushdownFilters, mergedFilters);
    }
    enableAllTextMode = fragmentContext.getOptions().getOption(ExecConstants.MONGO_ALL_TEXT_MODE).bool_val;
    enableNanInf = fragmentContext.getOptions().getOption(ExecConstants.JSON_READER_NAN_INF_NUMBERS).bool_val;
    readNumbersAsDouble = fragmentContext.getOptions().getOption(ExecConstants.MONGO_READER_READ_NUMBERS_AS_DOUBLE).bool_val;
    isBsonRecordReader = fragmentContext.getOptions().getOption(ExecConstants.MONGO_BSON_RECORD_READER).bool_val;
    logger.debug("BsonRecordReader is enabled? " + isBsonRecordReader);
    init(subScanSpec);
  }

  @Override
  protected Collection<SchemaPath> transformColumns(Collection<SchemaPath> projectedColumns) {
    Set<SchemaPath> transformed = Sets.newLinkedHashSet();
    if (!isStarQuery()) {
      for (SchemaPath column : projectedColumns) {
        String fieldName = column.getRootSegment().getPath();
        transformed.add(column);
        this.fields.put(fieldName, 1);
      }
    } else {
      // Tale all the fields including the _id
      this.fields.remove(DrillMongoConstants.ID);
      transformed.add(SchemaPath.STAR_COLUMN);
    }
    return transformed;
  }

  private void buildFilters(Document pushdownFilters,
      Map<String, List<Document>> mergedFilters) {
    for (Entry<String, List<Document>> entry : mergedFilters.entrySet()) {
      List<Document> list = entry.getValue();
      if (list.size() == 1) {
        this.filters.putAll(list.get(0));
      } else {
        Document andQueryFilter = new Document();
        andQueryFilter.put("$and", list);
        this.filters.putAll(andQueryFilter);
      }
    }
    if (pushdownFilters != null && !pushdownFilters.isEmpty()) {
      if (!mergedFilters.isEmpty()) {
        this.filters = MongoUtils.andFilterAtIndex(this.filters, pushdownFilters);
      } else {
        this.filters = pushdownFilters;
      }
    }
  }

  private void init(BaseMongoSubScanSpec subScanSpec) {
    List<String> hosts = subScanSpec.getHosts();
    List<ServerAddress> addresses = Lists.newArrayList();
    for (String host : hosts) {
      addresses.add(new ServerAddress(host));
    }
    MongoClient client = plugin.getClient(addresses);
    db = client.getDatabase(subScanSpec.getDbName());
    this.unionEnabled = fragmentContext.getOptions().getBoolean(ExecConstants.ENABLE_UNION_TYPE_KEY);
    collection = db.getCollection(subScanSpec.getCollectionName(), BsonDocument.class);
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.writer = new VectorContainerWriter(output, unionEnabled);
    // Default is BsonReader and all text mode will not be honored in
    // BsonRecordReader
    if (isBsonRecordReader) {
      this.bsonReader = new BsonRecordReader(fragmentContext.getManagedBuffer(), Lists.newArrayList(getColumns()),
          readNumbersAsDouble);
      logger.debug("Initialized BsonRecordReader. ");
    } else {
      this.jsonReader = new JsonReader.Builder(fragmentContext.getManagedBuffer())
          .schemaPathColumns(Lists.newArrayList(getColumns()))
          .allTextMode(enableAllTextMode)
          .readNumbersAsDouble(readNumbersAsDouble)
          .enableNanInf(enableNanInf)
          .build();
      logger.debug(" Intialized JsonRecordReader. ");
    }
  }
  
  static class BsonAttributes{
	  private BsonDocument document;
	  private int index;
	public BsonDocument getDocument() {
		return document;
	}
	public void setDocument(BsonDocument document) {
		this.document = document;
	}
	public int getIndex() {
		return index;
	}
	public void setIndex(int index) {
		this.index = index;
	}
	@Override
	public String toString() {
		return "BsonAttributes [document=" + document + ", index=" + index + "]";
	}
	
	
	  
  }

  private static BsonAttributes getFirstProject(List<Bson> operations) {
	  int index=0;
      for (Bson operation : operations) {
    	  index++;
          if (operation.toBsonDocument(Document.class, null).containsKey("$project")) {
        	  BsonAttributes attr=new BsonAttributes();
        	  attr.setDocument(operation.toBsonDocument(BsonDocument.class, null).get("$project").asDocument());
        	  attr.setIndex(index);
              return  attr;
          }
      }
      return null;
  }
  
  
  private static BsonAttributes getFirstGroup(List<Bson> operations) {
	  int index=0;
      for (Bson operation : operations) {
    	  index++;
          if (operation.toBsonDocument(Document.class, null).containsKey("$group")) {
        	  BsonAttributes attr=new BsonAttributes();
        	  attr.setDocument(operation.toBsonDocument(BsonDocument.class, null).get("$group").asDocument());
        	  attr.setIndex(index);
              return  attr;
          }
      }
      return null;
  }
  @Override
  public int next() {
    if (cursor == null) {
      logger.debug("Filters Applied : " + filters);
      logger.debug("Fields Selected :" + fields);
      logger.info(" fields {}, operations {} ",fields,operations);
      
      // big number charts
      if(fields.containsKey("_id") && (fields.containsKey(COUNT)||  fields.containsKey(COUNT_2))&& fields.size()==2) {
    	  	logger.info("first case");
			cursor=getCursorForBigNumbers();
      } 
      // bar or pie charts with a column and count
      else if(fields.containsKey("_id") && (fields.containsKey(COUNT)||  fields.containsKey(COUNT_2)) && fields.size()==3) {
    	 logger.info("second case");
    	 String field=fields.keySet().stream().filter(o-> !o.equals("_id") && !o.equals(COUNT)).findAny().orElse(null);
    	 logger.info("field {}",field);
    	 BsonAttributes project=getFirstProject(operations);
    	 logger.info("project {}",project);
    	 BsonValue reqVal= project.getDocument().get(field);
    	 logger.info("req val {}",reqVal);
    	 if(reqVal instanceof BsonString) {
    		 BsonAttributes bsonAttributes=getFirstGroup(operations);
    		 if(bsonAttributes!=null)
    			 bsonAttributes.getDocument().put("_id", new  BsonString(((BsonString)reqVal).getValue()));
    		 
        	 operations.remove(project.getIndex()-1);	 
    	 }else if(!(reqVal instanceof BsonDocument)){
        	 operations.remove(project.getIndex()-1);	 
    	 }
    	
    	 logger.info("updated operations {} ",operations);
    	 cursor= getProjection().batchSize(plugin.getConfig().getBatchSize()).iterator();
      } 
      else if(fields.containsKey("_id") && (fields.containsKey(COUNT)||  fields.containsKey(COUNT_2)) && fields.size()>3) {
     	 logger.info("third case");
     	 List<String> columns=fields.keySet().stream().filter(o-> !o.equals("_id") && !o.equals(COUNT)).collect(Collectors.toList());
     	 logger.info("fields {}",columns);
     	 BsonAttributes project=getFirstProject(operations);
     	 logger.info("project {}",project);
     	 columns.stream().map(o->{
     		ProjectionHelper projectionHelper=new ProjectionHelper();
     		projectionHelper.setKey(o);
     		projectionHelper.setValue(project.getDocument().get(o));
     		return projectionHelper;
     	 }).forEach(reqVal->{
     		if(reqVal.getValue()  instanceof BsonString) {
        		 BsonAttributes bsonAttributes=getFirstGroup(operations);
        		 if(bsonAttributes!=null){
        			 BsonDocument bsonDocument= (BsonDocument) bsonAttributes.getDocument().get("_id");
        			 bsonDocument.append(reqVal.getKey(), reqVal.getValue());
        		 }
        	 }
     	 });
     	 
     	 if(project!=null)
     		 operations.remove(project.getIndex()-1);	 
     	 
     	 logger.info("updated operations {} ",operations);
     	 cursor= getProjection().batchSize(plugin.getConfig().getBatchSize()).iterator();
      }
      // other cases
      else {
    	  logger.info("drill's default case");
          cursor = getProjection().batchSize(plugin.getConfig().getBatchSize()).iterator();
      }
    }

    writer.allocate();
    writer.reset();

    int docCount = 0;
    Stopwatch watch = Stopwatch.createStarted();

    try {
      while (docCount < BaseValueVector.INITIAL_VALUE_ALLOCATION && cursor.hasNext()) {
        writer.setPosition(docCount);
        if (isBsonRecordReader) {
          BsonDocument bsonDocument = cursor.next();
          bsonReader.write(writer, new BsonDocumentReader(bsonDocument));
          logger.info("bson {}",bsonDocument);
        } else {
          String doc = cursor.next().toJson();
          jsonReader.write(writer);
        }
        docCount++;
      }

      if (isBsonRecordReader) {
        bsonReader.ensureAtLeastOneField(writer);
      } else {
        jsonReader.ensureAtLeastOneField(writer);
      }

      writer.setValueCount(docCount);
      logger.info("Took {} ms to get {} records", watch.elapsed(TimeUnit.MILLISECONDS), docCount);
      return docCount;
    } catch (IOException e) {
      String msg = "Failure while reading document. - Parser was at record: " + (docCount + 1);
      logger.error(msg, e);
      throw new DrillRuntimeException(msg, e);
    }
  }
  
	static class ProjectionHelper {
		private String key;
		private BsonValue value;

		public String getKey() {
			return key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		public BsonValue getValue() {
			return value;
		}

		public void setValue(BsonValue value) {
			this.value = value;
		}

	}


  /**
   * @resposnible to get the projection
   * @return
   */
	private MongoIterable<BsonDocument> getProjection() {
		MongoIterable<BsonDocument> projection;
		if (CollectionUtils.isNotEmpty(operations)) {
			List<Bson> operations = new ArrayList<>(this.operations);
			if (!fields.isEmpty()) {
				operations.add(Aggregates.project(fields));
			}
			if (plugin.getConfig().allowDiskUse()) {
				projection = collection.aggregate(operations).allowDiskUse(true);
			} else {
				projection = collection.aggregate(operations);
			}
		} else {
			projection = collection.find(filters).projection(fields);
		}

		return projection;
	}

  
  /**
   * @responsible to get cursor for big number charts
   * @return
   */
private MongoCursor<BsonDocument> getCursorForBigNumbers() {
	return  new MongoCursor<BsonDocument>() {
		boolean isNextAvailable=true;
		
		@Override
		public void close() {
			//there is no need to close anything
		}

		@Override
		public boolean hasNext() {
			return isNextAvailable;
		}

		@Override
		public BsonDocument next() {
			isNextAvailable=false;
			Bson filter= operations.stream().filter(o->o.toBsonDocument(BsonDocument.class,null).containsKey("$match")).findAny().orElse(null);
			logger.info("filter {}",filter);
			Document input= new Document("count", collection.getNamespace().getCollectionName());
			
			if(filter!=null){
				Bson finalFilter=(Bson)filter.toBsonDocument(BsonDocument.class,null).get("$match");
				input=  input.append("query", finalFilter);
			}
			
			Document result= db.runCommand(input);
			return new Document(COUNT, result.getInteger("n")).toBsonDocument();
		}
                                             
		@Override
		public BsonDocument tryNext() {
			return null;
		}

		@Override
		public ServerCursor getServerCursor() {
			return null;
		}

		@Override
		public ServerAddress getServerAddress() {
			return null;
		}

	};
}

  @Override
  public void close() {
  }

  @Override
  public String toString() {
    Object reader = isBsonRecordReader ? bsonReader : jsonReader;
    return "MongoRecordReader[reader=" + reader + "]";
  }
}

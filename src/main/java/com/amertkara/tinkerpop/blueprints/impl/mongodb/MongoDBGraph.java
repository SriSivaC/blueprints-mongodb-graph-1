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
package com.amertkara.tinkerpop.blueprints.impl.mongodb;

import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBRef;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoWriteConcernException;
import com.mongodb.MongoWriteException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBGraph implements MetaGraph<MongoDatabase>, KeyIndexableGraph {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBGraph.class.getName());
    private static final Features FEATURES = new Features();

    private MongoClient mongoClient;
    private MongoDatabase rawGraph;
    private String edgeCollection;
    private String vertexCollection;
    
    static {
        // TODO: put the features
    }
    
    /**
     * TODO: Implement constructors that take {@link Properties} and {@link MongoClientOptions}
     * 
     * @param host
     * @param port
     * @param databaseName
     * @param edgeCollection
     * @param vertexCollection
     */
    public MongoDBGraph(final String host, final int port, 
            final String databaseName, final String edgeCollection,
            final String vertexCollection) {
        
        if (edgeCollection == null || edgeCollection.isEmpty() ||
                vertexCollection == null || vertexCollection.isEmpty() ||
                databaseName == null || databaseName.isEmpty()) {
            throw new IllegalStateException("edgeCollection, vertexCollection and databaseName cannot be empty.");
        }
        
        if (host != null && !host.isEmpty()) {
            mongoClient = new MongoClient(host, port);
        } else {
           mongoClient = new MongoClient();
        }  

        this.edgeCollection = edgeCollection;
        this.vertexCollection = vertexCollection;
        this.rawGraph = mongoClient.getDatabase(databaseName);
        
        this.rawGraph.getCollection(edgeCollection).createIndex(new Document(MongoDBConstants.FIELD_ID, new Integer(1)).append("unique", new Boolean(true)));
        this.rawGraph.getCollection(vertexCollection).createIndex(new Document(MongoDBConstants.FIELD_ID, new Integer(1)).append("unique", new Boolean(true)));
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getFeatures()
     */
    @Override
    public Features getFeatures() {
        return FEATURES;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#addVertex(java.lang.Object)
     */
    @Override
    public Vertex addVertex(Object id) throws MongoWriteException, MongoWriteConcernException {
        if (id == null) {
            id = getCollectionNextID(vertexCollection);
        }
        
        MongoDBVertexIterable vertexIt;
        MongoDBVertex vertex;
        
        // Insert the new edge
        this.rawGraph.getCollection(vertexCollection).insertOne(
                new Document(MongoDBConstants.FIELD_ID, id)
        );
        
        // TODO: Performance can be improved by getting rid of the second call.
        // Fetch the newly created edge
        vertexIt = new MongoDBVertexIterable(this.rawGraph.getCollection(vertexCollection).find(new Document(MongoDBConstants.FIELD_ID, id)), this);
        
        try {
            vertex = (MongoDBVertex) vertexIt.iterator().next();
        } catch (NoSuchElementException nsee) {
            logger.warn("Newly created vertex could not be fetched. The error message: " + nsee.getMessage());
            vertex = null;
        }
            
        return vertex;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getVertex(java.lang.Object)
     */
    @Override
    public Vertex getVertex(Object id) {
        if (null == id) {
            throw ExceptionFactory.vertexIdCanNotBeNull();
        }
        
        MongoDBVertex vertex;
        MongoDBVertexIterable vertexIt;
        
        try {
            if (id instanceof Long) {
                id = (Long) id;
            } else if (id instanceof Number) {
                id = new Long(((Number)id).longValue());
            } else {
                id = new Long(Double.valueOf(id.toString()).longValue());
            }
            
            vertexIt = new MongoDBVertexIterable(this.rawGraph.getCollection(vertexCollection).find(new Document(MongoDBConstants.FIELD_ID, id)), this);
            vertex = (MongoDBVertex) vertexIt.iterator().next();
        } catch (NoSuchElementException nsee) {
            logger.warn("Vertex could not be fetched. The error message: " + nsee.getMessage());
            vertex = null;
        } catch (NumberFormatException nfe) {
            logger.warn("Given id could not be converted to Long. The error message:  " + nfe.getMessage());
            vertex = null;
        }
        
        return vertex;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.tinkerpop.blueprints.Graph#removeVertex(com.tinkerpop.blueprints.
     * Vertex)
     */
    @Override
    public void removeVertex(Vertex vertex) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getVertices()
     */
    @Override
    public Iterable<Vertex> getVertices() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getVertices(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#addEdge(java.lang.Object,
     * com.tinkerpop.blueprints.Vertex, com.tinkerpop.blueprints.Vertex,
     * java.lang.String)
     */
    @Override
    public Edge addEdge(Object id, Vertex outVertex, Vertex inVertex, String label) throws MongoWriteException, MongoWriteConcernException {
        if (label == null) {
            throw ExceptionFactory.edgeLabelCanNotBeNull();
        }
        
        if (id == null) {
            id = getCollectionNextID(edgeCollection);
        }
        
        MongoDBEdgeIterable edgeIt;
        MongoDBEdge edge;
        
        // Insert the new edge
        this.rawGraph.getCollection(edgeCollection).insertOne(
                new Document(MongoDBConstants.FIELD_ID, id)
                .append(MongoDBConstants.FIELD_LABEL, label)
                .append(MongoDBConstants.FIELD_OUTV, new DBRef(vertexCollection, outVertex.getId()))
                .append(MongoDBConstants.FIELD_INV, new DBRef(vertexCollection, inVertex.getId()))
        );
        
        // Fetch the newly created edge
        edgeIt = new MongoDBEdgeIterable(this.rawGraph.getCollection(edgeCollection).find(new Document(MongoDBConstants.FIELD_ID, id)), this);
        
        try {
            edge = (MongoDBEdge) edgeIt.iterator().next();
        } catch (NoSuchElementException nsee) {
            logger.warn("Newly created edge could not be fetched. The error message: " + nsee.getMessage());
            edge = null;
        }
            
        return edge;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getEdge(java.lang.Object)
     */
    @Override
    public Edge getEdge(Object id) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.tinkerpop.blueprints.Graph#removeEdge(com.tinkerpop.blueprints.Edge)
     */
    @Override
    public void removeEdge(Edge edge) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getEdges()
     */
    @Override
    public Iterable<Edge> getEdges() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#getEdges(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#query()
     */
    @Override
    public GraphQuery query() {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.tinkerpop.blueprints.Graph#shutdown()
     */
    @Override
    public void shutdown() {
        mongoClient.close();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.tinkerpop.blueprints.KeyIndexableGraph#dropKeyIndex(java.lang.String,
     * java.lang.Class)
     */
    @Override
    public <T extends Element> void dropKeyIndex(String key,
            Class<T> elementClass) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.tinkerpop.blueprints.KeyIndexableGraph#createKeyIndex(java.lang.String
     * , java.lang.Class, com.tinkerpop.blueprints.Parameter[])
     */
    @Override
    public <T extends Element> void createKeyIndex(String key,
            Class<T> elementClass, Parameter... indexParameters) {
        // TODO Auto-generated method stub

    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.tinkerpop.blueprints.KeyIndexableGraph#getIndexedKeys(java.lang.Class
     * )
     */
    @Override
    public <T extends Element> Set<String> getIndexedKeys(Class<T> elementClass) {
        // TODO Auto-generated method stub
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * com.tinkerpop.blueprints.MetaGraph#getRawGraph()
     */
    @Override
    public MongoDatabase getRawGraph() {
        return this.rawGraph;
    }
    
    /**
     * @return the edge collection
     */
    public MongoCollection<Document> getEdgeCollection() {
        return this.rawGraph.getCollection(edgeCollection);
    }
    
    /**
     * @return the vertex collection
     */
    public MongoCollection<Document> getVertexCollection() {
        return this.rawGraph.getCollection(vertexCollection);
    }
    
    /**
     * Gets the next avaliable id {@link MongoDBConstants#FIELD_ID} in a given
     * collection.
     * 
     * @param collectionName
     * @return Integer
     */
    public Integer getCollectionNextID (String collectionName) {
        Integer lastId = null;
        
        try {
            lastId = (Integer) this.rawGraph.getCollection(collectionName).find().sort(new Document(MongoDBConstants.FIELD_ID, new Integer(-1))).iterator().next().get(MongoDBConstants.FIELD_ID);
            return Integer.valueOf(lastId.intValue() + 1);
        } catch (NoSuchElementException nsee) {
            lastId = Integer.valueOf(1);
        }
        
        return lastId;
    }
}

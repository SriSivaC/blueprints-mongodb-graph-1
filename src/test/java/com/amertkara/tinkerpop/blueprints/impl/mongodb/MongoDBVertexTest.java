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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBConstants;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB;
import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBVertexTest extends InMemoryMongoDB {
    private FindIterable<Document> cursor;
    private List<String> edgeLabels;
    private List<String> vertexLabels;
    private Iterable<Edge> edges;
    private Iterable<Vertex> vertices;
    private Iterator<Edge> itEdges;
    private Iterator<Vertex> itVertices;
    private MongoDBGraph graphDB;
    
    /* (non-Javadoc)
     * @see com.amertkara.pgss.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB#setUp()
     */
    @SuppressWarnings("boxing")
    @Before
    public void setUp() throws Exception {
        super.setUp();
        graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
        // Insert three vertices via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("name", "pilot")));
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 2).append(MongoDBConstants.FIELD_PROPERTIES, new Document("name", "plane")));
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 3).append(MongoDBConstants.FIELD_PROPERTIES, new Document("name", "mechanic")));
        
        // Insert two edges via MongoDB API
        // Link those vertices with edges flies, maintains, knows relations
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1)
        .append(MongoDBConstants.FIELD_LABEL, "flies")
        .append(MongoDBConstants.FIELD_OUTV, new DBRef(COLLECTION_VERTICES, 1))
        .append(MongoDBConstants.FIELD_INV, new DBRef(COLLECTION_VERTICES, 2)));
        
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 2)
        .append(MongoDBConstants.FIELD_LABEL, "maintains")
        .append(MongoDBConstants.FIELD_OUTV, new DBRef(COLLECTION_VERTICES, 3))
        .append(MongoDBConstants.FIELD_INV, new DBRef(COLLECTION_VERTICES, 2)));
        
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 2)
        .append(MongoDBConstants.FIELD_LABEL, "knows")
        .append(MongoDBConstants.FIELD_OUTV, new DBRef(COLLECTION_VERTICES, 1))
        .append(MongoDBConstants.FIELD_INV, new DBRef(COLLECTION_VERTICES, 3)));
    }

    /* (non-Javadoc)
     * @see com.amertkara.pgss.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB#tearDown()
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getEdges(com.tinkerpop.blueprints.Direction, java.lang.String[])}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetEdgesOutV() throws Exception {
        // Create a vertex object from the pilot vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBVertex pilot = new MongoDBVertex(cursor.iterator().next(), graphDB);

        edges = pilot.getEdges(Direction.OUT, new String[]{"flies", "maintains"});
        itEdges = edges.iterator();
        assertEquals("flies", ((MongoDBEdge)itEdges.next()).getLabel());
        
        // Create a vertex object from the plane vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 2));
        MongoDBVertex plane = new MongoDBVertex(cursor.iterator().next(), graphDB);
        // plane has no outgoing edge
        assertFalse(plane.getEdges(Direction.OUT, new String[]{"knows", "flies", "maintains"}).iterator().hasNext());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getEdges(com.tinkerpop.blueprints.Direction, java.lang.String[])}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetEdgesInV() throws Exception {
        // Create a vertex object from the plane vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 2));
        MongoDBVertex plane = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        edges = plane.getEdges(Direction.IN, new String[]{"flies", "maintains"});
        itEdges = edges.iterator();
        
        edgeLabels = new ArrayList<String>();
        while (itEdges.hasNext()) {
            edgeLabels.add(((MongoDBEdge) itEdges.next()).getLabel());
        }
        
        assertTrue(edgeLabels.containsAll(Arrays.asList("flies", "maintains")));
        
        // Create a vertex object from the pilot vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBVertex pilot = new MongoDBVertex(cursor.iterator().next(), graphDB);
        // pilot has no incoming edge
        assertFalse(pilot.getEdges(Direction.IN, new String[]{"knows", "flies", "maintains"}).iterator().hasNext());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getEdges(com.tinkerpop.blueprints.Direction, java.lang.String[])}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetEdgesBothV() throws Exception {
        // Create a vertex object from the mechanic vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 3));
        MongoDBVertex mechanic = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        edges = mechanic.getEdges(Direction.BOTH, new String[]{"flies", "maintains", "knows"});
        itEdges = edges.iterator();
        
        edgeLabels = new ArrayList<String>();
        while (itEdges.hasNext()) {
            edgeLabels.add(((MongoDBEdge) itEdges.next()).getLabel());
        }
        
        assertTrue(edgeLabels.containsAll(Arrays.asList("knows", "maintains")));
        assertFalse(edgeLabels.containsAll(Arrays.asList("flies")));
        
        // Create a vertex object from the plane vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 2));
        MongoDBVertex plane = new MongoDBVertex(cursor.iterator().next(), graphDB);
        // plane cannot possibly know somebody or be known.
        assertFalse(plane.getEdges(Direction.BOTH, new String[]{"knows"}).iterator().hasNext());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetVerticesOutV() throws Exception {
        // Create a vertex object from the pilot vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 2));
        MongoDBVertex plane = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        vertices = plane.getVertices(Direction.OUT, new String[]{"flies", "maintains", "knows"});
        itVertices = vertices.iterator();
        
        vertexLabels = new ArrayList<String>();
        while (itVertices.hasNext()) {
            Vertex vertex = (Vertex) itVertices.next();
            vertexLabels.add((String)((MongoDBVertex)vertex).getProperty("name"));
        }
        
        assertTrue(vertexLabels.containsAll(Arrays.asList("pilot", "mechanic")));
        assertFalse(vertexLabels.containsAll(Arrays.asList("plane")));
        
        // Create a vertex object from the pilot vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBVertex pilot = new MongoDBVertex(cursor.iterator().next(), graphDB);
        // pilot cannot be known, flew or maintained any node
        assertFalse(pilot.getVertices(Direction.OUT, new String[]{"flies", "maintains", "knows"}).iterator().hasNext());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetVerticesInV() throws Exception {
        // Create a vertex object from the plane vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBVertex pilot = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        vertices = pilot.getVertices(Direction.IN, new String[]{"flies", "maintains", "knows"});
        itVertices = vertices.iterator();
        
        vertexLabels = new ArrayList<String>();
        while (itVertices.hasNext()) {
            Vertex vertex = (Vertex) itVertices.next();
            vertexLabels.add((String)((MongoDBVertex)vertex).getProperty("name"));
        }
        
        assertTrue(vertexLabels.containsAll(Arrays.asList("plane", "mechanic")));
        assertFalse(vertexLabels.containsAll(Arrays.asList("pilot")));
        
        // Create a vertex object from the plane vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 2));
        MongoDBVertex plane = new MongoDBVertex(cursor.iterator().next(), graphDB);
        // plane cannot possibly know, maintain or fly any node.
        assertFalse(plane.getVertices(Direction.IN, new String[]{"flies", "maintains", "knows"}).iterator().hasNext());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getVertices(com.tinkerpop.blueprints.Direction, java.lang.String[])}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetVerticesBothV() throws Exception {
        // Create a vertex object from the mechanic vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 3));
        MongoDBVertex mechanic = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        vertices = mechanic.getVertices(Direction.BOTH, new String[]{"flies", "maintains", "knows"});
        itVertices = vertices.iterator();
        
        vertexLabels = new ArrayList<String>();
        while (itVertices.hasNext()) {
            Vertex vertex = (Vertex) itVertices.next();
            vertexLabels.add((String)((MongoDBVertex)vertex).getProperty("name"));
        }
        
        assertTrue(vertexLabels.containsAll(Arrays.asList("plane", "pilot")));
        assertFalse(vertexLabels.containsAll(Arrays.asList("mechanic")));
        
        // Create a new vertex that is not connected to any vertex
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 4).append(MongoDBConstants.FIELD_PROPERTIES, new Document("name", "tower")));
        // Create a vertex object from the plane vertex
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 4));
        MongoDBVertex tower = new MongoDBVertex(cursor.iterator().next(), graphDB);
        // tower cannot possibly know, maintain or fly any node.
        assertFalse(tower.getVertices(Direction.IN, new String[]{"flies", "maintains", "knows"}).iterator().hasNext());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex#getLabel()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetLabel() throws Exception {
        // Insert a vertex via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 10).append(MongoDBConstants.FIELD_LABEL, "person").append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create a vertex object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 10));
        MongoDBVertex vertex = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        assertEquals("person", vertex.getLabel());
    }
}

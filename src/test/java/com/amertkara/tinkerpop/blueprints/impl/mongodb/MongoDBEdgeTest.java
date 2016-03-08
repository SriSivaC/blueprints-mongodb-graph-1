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

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBConstants;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB;
import com.mongodb.DBRef;
import com.mongodb.client.FindIterable;
import com.tinkerpop.blueprints.Direction;

import static org.junit.Assert.assertEquals;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBEdgeTest extends InMemoryMongoDB {
    private MongoDBGraph graphDB;

    /**
     * @throws java.lang.Exception void
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
        graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
    }

    /**
     * @throws java.lang.Exception void
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge#getMongoCollection()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetMongoCollection() {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_LABEL, "person").append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBEdge edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals(this.graphDB.getEdgeCollection().getNamespace(), edge.getMongoCollection().getNamespace());
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge#getVertex(com.tinkerpop.blueprints.Direction)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetVertex() {
        // Insert two vertices via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("test", "abc")));
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 2).append(MongoDBConstants.FIELD_PROPERTIES, new Document("test", "def")));
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_INV, new DBRef(COLLECTION_VERTICES, 1)).append(MongoDBConstants.FIELD_OUTV, new DBRef(COLLECTION_VERTICES, 2)));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBEdge edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals("def", edge.getVertex(Direction.OUT).getProperty("test"));
        assertEquals("abc", edge.getVertex(Direction.IN).getProperty("test"));
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge#getLabel()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetLabel() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_LABEL, "person").append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBEdge edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals("person", edge.getLabel());
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge#getinV()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetinV() {
        // Insert a vertex via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("test", "abc")));
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_INV, new DBRef(COLLECTION_VERTICES, 1)));

        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBEdge edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals("abc", edge.getinV().getProperty("test"));
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge#getOutV()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetOutV() throws Exception {
        // Insert a vertex via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("test", "abc")));
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_OUTV, new DBRef(COLLECTION_VERTICES, 1)));

        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBEdge edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals("abc", edge.getOutV().getProperty("test"));
    }
}

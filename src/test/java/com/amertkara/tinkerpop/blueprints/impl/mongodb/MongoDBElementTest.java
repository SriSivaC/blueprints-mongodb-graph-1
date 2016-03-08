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

import java.util.HashSet;
import java.util.Set;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBConstants;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB;
import com.mongodb.client.FindIterable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBElementTest extends InMemoryMongoDB {
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
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#getProperty(java.lang.String)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetProperty() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("name", "1")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals("1", edge.getProperty("name"));
        
        // Remove the edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).deleteOne(new Document(MongoDBConstants.FIELD_ID, 1));
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#getPropertyKeys()}.
     */
    @SuppressWarnings("boxing")
    @Test 
    public void testGetPropertyKeys() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        Set<String> keys = new HashSet<String>();
        keys.add("key1");
        keys.add("key2");
        
        assertEquals(keys, edge.getPropertyKeys());
        
        // Remove the edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).deleteOne(new Document(MongoDBConstants.FIELD_ID, 1));
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#setProperty(java.lang.String, java.lang.Object)}.
     */
    @Test
    @SuppressWarnings("boxing")
    public void testSetProperty() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
    
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        edge.setProperty("key1", "val3");
        
        // Pull the modified 
        cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        Document modified = cursor.iterator().next();
        assertEquals("val3", ((Document) modified.get(MongoDBConstants.FIELD_PROPERTIES)).get("key1"));
        assertEquals("val3", edge.getProperty("key1"));
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#removeProperty(java.lang.String)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testRemoveProperty() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        edge.removeProperty("key1");
        
        // Pull the modified
        cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        Document modified = cursor.iterator().next();
        
        assertNull(edge.getProperty("key1"));
        assertNull(modified.get(new Document(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", ""))));
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#reload()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testReload() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        // Update the record via API
        this.mongoDB.getCollection(COLLECTION_EDGES).updateOne(new Document(MongoDBConstants.FIELD_ID, 1), new Document("$set", new Document(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val4"))));
        
        edge.reload();
        
        assertEquals("val4", edge.getProperty("key1"));
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#getId()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetId() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        
        assertEquals(1, edge.getId());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#remove()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testRemove() throws Exception {
        // Insert an edge via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
        
        // Create an edge object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBElement edge = new MongoDBEdge(cursor.iterator().next(), graphDB);
        edge.remove();
        
        cursor = this.mongoDB.getCollection(COLLECTION_EDGES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        assertFalse(cursor.iterator().hasNext());
    }
}

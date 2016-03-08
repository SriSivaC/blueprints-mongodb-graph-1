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
package com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded;

import java.util.Iterator;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import static org.junit.Assert.assertEquals;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class InMemoryMongoDBTest extends InMemoryMongoDB {
    
    /**
     * @throws java.lang.Exception void
     */
    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    /**
     * @throws java.lang.Exception void
     */
    @After
    public void tearDown() throws Exception {
        super.tearDown();
    }

    /**
     * @throws Exception void
     */
    @SuppressWarnings("boxing")
    @Test
    public void testCreateVertices() throws Exception {
        MongoCollection<Document> vertices = mongoDB.getCollection(COLLECTION_VERTICES);
        
        vertices.insertOne(
                new Document("id", "1")
                .append("label", "person")
                .append("properties", new Document().append("firstName", "Alex").append("lastName", "Ferguson"))
        );
        vertices.insertOne(
                new Document("id", "2")
                .append("label", "person")
                .append("properties", new Document().append("firstName", "Iker").append("lastName", "Casillas"))
        );
        vertices.createIndex(new BasicDBObject("properties.firstName", 1));
        vertices.createIndex(new BasicDBObject("properties.firstName", 1));
        
        FindIterable<Document> cursor = vertices.find(new Document("properties.firstName", "Alex"));
        Iterator<Document> it = cursor.iterator();
        
        assertEquals(true, it.hasNext());
        while(it.hasNext()) {
            Document object = it.next();
            assertEquals("Ferguson", ((Document) object.get("properties")).get("lastName"));
        }
    }
    
    /**
     * @throws Exception void
     */
    @SuppressWarnings("boxing")
    @Test
    public void testCreateEdges() throws Exception {
        MongoCollection<Document> edges = mongoDB.getCollection(COLLECTION_EDGES);
        
        edges.insertOne(
                new Document("id", "1")
                .append("label", "knows")
                .append("inV", new Document())
                .append("outV", new Document())
                .append("properties", new Document().append("weight", "0.6"))
        );
        edges.createIndex(new Document("properties.weight", 1));
        
        FindIterable<Document> cursor = edges.find( new Document("properties.weight", "0.6"));
        Iterator<Document> it = cursor.iterator();
        
        assertEquals(true, it.hasNext());
        while(it.hasNext()) {
            Document object = it.next();
            assertEquals("0.6", ((Document) object.get("properties")).get("weight"));
        }
    }
}

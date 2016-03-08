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
import java.util.Iterator;
import java.util.List;

import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBConstants;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdge;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBVertex;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB;
import com.mongodb.client.FindIterable;
import com.mongodb.client.ListIndexesIterable;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBGraphTest extends InMemoryMongoDB {

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
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#MongoDBGraph(java.lang.String, int, java.lang.String, java.lang.String, java.lang.String)}.
     */
    @Test
    public void testMongoDBGraph() {
        ListIndexesIterable<Document> indexes;
        Iterator<Document> it;
        List<String> indexNames;
        
        // Try initiating the graph database
        MongoDBGraph graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);

        assertNotNull(graphDB.getRawGraph());
        assertNotNull(graphDB.getRawGraph().getCollection(COLLECTION_EDGES));
        assertNotNull(graphDB.getRawGraph().getCollection(COLLECTION_VERTICES));
        
        indexes = graphDB.getRawGraph().getCollection(COLLECTION_EDGES).listIndexes();
        it = indexes.iterator();
        indexNames = new ArrayList<String>();
        
        while (it.hasNext()) {
            Document document = (Document) it.next();
            indexNames.add((String)document.get("name"));
        }
        assertTrue(indexNames.contains("id_1_unique_"));
        
        indexes = graphDB.getRawGraph().getCollection(COLLECTION_VERTICES).listIndexes();
        it = indexes.iterator();
        indexNames.clear();
        
        while (it.hasNext()) {
            Document document = (Document) it.next();
            indexNames.add((String)document.get("name"));
        }
        assertTrue(indexNames.contains("id_1_unique_"));
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getFeatures()}.
     */
    @Ignore
    public void testGetFeatures() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#addVertex(java.lang.Object)}.
     */
    @Test
    public void testAddVertex() {
        Integer id = new Integer(1);
        
        // Initiate the graph database
        MongoDBGraph graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
        graphDB.addVertex(id);
        
        FindIterable<Document> result = graphDB.getRawGraph().getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, id));
        assertTrue(result.iterator().hasNext());
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getVertex(java.lang.Object)}.
     */
    @Test
    public void testGetVertex() {
        Integer id = new Integer(1);
        // Initiate the graph database
        MongoDBGraph graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
        
        // No vertex exists
        assertNull(graphDB.getVertex(id));
        
        // Insert a vertex via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, id).append(MongoDBConstants.FIELD_LABEL, "person").append(MongoDBConstants.FIELD_PROPERTIES, new Document("key1", "val1").append("key2", "val2")));
    
        MongoDBVertex vertex = (MongoDBVertex) graphDB.getVertex(id);
        assertEquals("person", vertex.getLabel());
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#removeVertex(com.tinkerpop.blueprints.Vertex)}.
     */
    @Ignore
    public void testRemoveVertex() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getVertices()}.
     */
    @Ignore
    public void testGetVertices() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getVertices(java.lang.String, java.lang.Object)}.
     */
   	@Ignore
    public void testGetVerticesStringObject() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#addEdge(java.lang.Object, com.tinkerpop.blueprints.Vertex, com.tinkerpop.blueprints.Vertex, java.lang.String)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testAddEdge() {
        // Initiate the graph database
        MongoDBGraph graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
        // Create the out/in vertices object
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1));
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 2));

        // Create the out vertex object
        FindIterable<Document> cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 1));
        MongoDBVertex outVertex = new MongoDBVertex(cursor.iterator().next(), graphDB);
        cursor = this.mongoDB.getCollection(COLLECTION_VERTICES).find(new Document(MongoDBConstants.FIELD_ID, 2));
        MongoDBVertex inVertex = new MongoDBVertex(cursor.iterator().next(), graphDB);
        
        // Add the edge
        MongoDBEdge edge = (MongoDBEdge) graphDB.addEdge(null, outVertex, inVertex, "tesla");
        
        assertEquals("tesla", edge.getLabel());
    }
    
    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#addEdge(java.lang.Object, com.tinkerpop.blueprints.Vertex, com.tinkerpop.blueprints.Vertex, java.lang.String)}.
     */
    @Test(expected = IllegalArgumentException.class)
    public void testAddEdgeLabelCannotBeNull() {
        // Initiate the graph database
        MongoDBGraph graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
        
        graphDB.addEdge(new Integer(1), new MongoDBVertex(new Document(), graphDB), new MongoDBVertex(new Document(), graphDB), null);
    }

    /**
     * Test method for {@link com.amertkara.pgss.tinkerpop.bluep,rints.impl.mongodb.MongoDBGraph#getEdge(java.lang.Object)}.
     */
    @Ignore
    public void testGet() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#removeEdge(com.tinkerpop.blueprints.Edge)}.
     */
    @Ignore
    public void testRemoveEdge() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getEdges()}.
     */
    @Ignore
    public void testGetEdges() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getEdges(java.lang.String, java.lang.Object)}.
     */
    @Ignore
    public void testGetEdgesStringObject() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#query()}.
     */
    @Ignore
    public void testQuery() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#shutdown()}.
     */
    @Ignore
    public void testShutdown() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#dropKeyIndex(java.lang.String, java.lang.Class)}.
     */
    @Ignore
    public void testDropKeyIndex() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#createKeyIndex(java.lang.String, java.lang.Class, com.tinkerpop.blueprints.Parameter[])}.
     */
    @Ignore
    public void testCreateKeyIndex() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getIndexedKeys(java.lang.Class)}.
     */
    @Ignore
    public void testGetIndexedKeys() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getRawGraph()}.
     */
    @Ignore
    public void testGetRawGraph() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getEdgeCollection()}.
     */
    @Ignore
    public void testGetEdgeCollection() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getVertexCollection()}.
     */
    @Ignore
    public void testGetVertexCollection() {
        fail("Not yet implemented");
    }

    /**
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph#getCollectionNextID(java.lang.String)}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testGetCollectionNextID() {
        // Initiate the graph database
        MongoDBGraph graphDB = new MongoDBGraph(HOST, PORT, DB_NAME, COLLECTION_EDGES, COLLECTION_VERTICES);
        
        assertEquals(new Integer(1), graphDB.getCollectionNextID(COLLECTION_VERTICES));
        
        // Insert vertices via MongoDB API
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1));
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1231));
        this.mongoDB.getCollection(COLLECTION_VERTICES).insertOne(new Document(MongoDBConstants.FIELD_ID, 131312));
        
        assertEquals(new Integer(131313), graphDB.getCollectionNextID(COLLECTION_VERTICES));
    }

}

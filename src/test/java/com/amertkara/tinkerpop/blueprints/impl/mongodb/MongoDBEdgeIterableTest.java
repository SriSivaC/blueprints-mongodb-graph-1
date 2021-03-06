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
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdgeIterable;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBGraph;
import com.amertkara.tinkerpop.blueprints.impl.mongodb.embedded.InMemoryMongoDB;
import com.mongodb.client.FindIterable;

import static org.junit.Assert.assertEquals;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBEdgeIterableTest extends InMemoryMongoDB {
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
     * Test method for {@link com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBEdgeIterable#iterator()}.
     */
    @SuppressWarnings("boxing")
    @Test
    public void testIterator() {
        // Insert some documents via MongoDB API
        this.mongoDB.getCollection(COLLECTION_EDGES).insertOne(new Document(MongoDBConstants.FIELD_ID, 1).append(MongoDBConstants.FIELD_LABEL, "1"));
        
        FindIterable<Document> result = this.graphDB.getEdgeCollection().find(new Document());
        MongoDBEdgeIterable edges = new MongoDBEdgeIterable(result, this.graphDB);
        
        assertEquals("1", ((MongoDBEdge)edges.iterator().next()).getLabel());
    }
}

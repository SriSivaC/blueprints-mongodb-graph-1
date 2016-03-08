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

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DBRef;
import com.mongodb.client.MongoCollection;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public class MongoDBEdge extends MongoDBElement implements Edge {
    private static Logger logger = LoggerFactory.getLogger(MongoDBEdge.class.getName());
    
    public MongoDBEdge(final Document edge, final MongoDBGraph graph) {
        super(edge, graph);
    }

    /* (non-Javadoc)
     * @see com.amertkara.tinkerpop.blueprints.impl.mongodb.MongoDBElement#getMongoCollection()
     */
    @Override
    public MongoCollection<Document> getMongoCollection() {
        return this.graph.getEdgeCollection();
    }

    @Override
    public Vertex getVertex(final Direction direction)
            throws IllegalArgumentException {
        if (direction.equals(Direction.OUT)) {
            return getOutV();
        } else if (direction.equals(Direction.IN)) {
            return getinV();
        } else {
            throw ExceptionFactory.bothIsNotSupported();
        }
    }

    @Override
    public String getLabel() {
        return this.rawElement.getString(MongoDBConstants.FIELD_LABEL);
    }

    public MongoDBVertex getOutV() {
        DBRef outV = (DBRef) this.rawElement.get(MongoDBConstants.FIELD_OUTV);
        Document vertex;
        try{
            vertex = this.graph.getVertexCollection().find(new Document(MongoDBConstants.FIELD_ID, outV.getId())).iterator().next();
        } catch (NoSuchElementException nsee) {
            logger.warn("outV of the edge " + getId() + " doesn't exist. Vertex id is " + outV.getId());
            return null;
        }
        return new MongoDBVertex(vertex, graph);
    }
    
    public MongoDBVertex getinV() {
        DBRef inV = (DBRef) this.rawElement.get(MongoDBConstants.FIELD_INV);
        Document vertex;
        try{
            vertex = this.graph.getVertexCollection().find(new Document(MongoDBConstants.FIELD_ID, inV.getId())).iterator().next();
        } catch (NoSuchElementException nsee) {
            logger.warn("inV of the edge " + getId() + " doesn't exist. Vertex id is " + inV.getId());
            return null;
        }
        return new MongoDBVertex(vertex, graph);
    }
}

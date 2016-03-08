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

import java.util.Set;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.result.UpdateResult;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.ElementHelper;

/**
 * @author Mert Kara (https://github.com/amertkara)
 * @since 0.1.0
 */
public abstract class MongoDBElement implements Element {
    private static Logger logger = LoggerFactory.getLogger(MongoDBElement.class.getName());
    protected final MongoDBGraph graph;
    protected Document rawElement;
    protected Object id;
    
    public abstract MongoCollection<Document> getMongoCollection();
    
    public MongoDBElement(final Document rawElement, final MongoDBGraph graph) {
        this.rawElement = rawElement;
        this.graph = graph;
    }
    
    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.Element#getProperty(java.lang.String)
     */
    @SuppressWarnings("unchecked")
    @Override
    public <T> T getProperty(final String key) {
        return (T) ((Document) this.rawElement.get(MongoDBConstants.FIELD_PROPERTIES)).get(key);
    }

    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.Element#getPropertyKeys()
     */
    @Override
    public Set<String> getPropertyKeys() {
        return ((Document) this.rawElement.get(MongoDBConstants.FIELD_PROPERTIES)).keySet();
    }

    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.Element#setProperty(java.lang.String, java.lang.Object)
     */
    @Override
    public void setProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        UpdateResult result = getMongoCollection().updateOne(this.rawElement, new Document("$set", new Document(MongoDBConstants.FIELD_PROPERTIES, new Document(key, value))));
        if (result.getMatchedCount() == result.getModifiedCount()) {
            logger.info("Property of element " + this.rawElement.get(MongoDBConstants.FIELD_ID) + " is set.");
        }
        // Refresh the rawElement
        this.reload();
    }

    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.Element#removeProperty(java.lang.String)
     */
    @Override
    public <T> T removeProperty(final String key) {
        if (getProperty(key) == null) {
            return null;
        } else {
            UpdateResult result = getMongoCollection().updateOne(this.rawElement, new Document("$unset", new Document(MongoDBConstants.FIELD_PROPERTIES + "." + key, "")));
            if (result.getMatchedCount() == result.getModifiedCount()) {
                logger.info("Property of element " + this.rawElement.get(MongoDBConstants.FIELD_ID) + " is unset.");
            }
            T removedProperty = this.getProperty(key);
            // Refresh the rawElement
            reload();
            return removedProperty;
        }
    }

    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.Element#remove()
     */
    @Override
    public void remove() {
        getMongoCollection().deleteOne(new Document("id", this.rawElement.get(MongoDBConstants.FIELD_ID)));
    }

    /* (non-Javadoc)
     * @see com.tinkerpop.blueprints.Element#getId()
     */
    @Override
    public Object getId() {
        return this.rawElement.get(MongoDBConstants.FIELD_ID);
    }
    
    /* (non-Javadoc)
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        return getId().hashCode();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(final Object obj) {
        return ElementHelper.areEqual(this, obj);
    }

    public void reload() {
        this.rawElement = getMongoCollection().find(new Document("id", this.rawElement.get(MongoDBConstants.FIELD_ID))).iterator().next();
    }
    
    
}

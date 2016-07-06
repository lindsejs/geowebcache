/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.geowebcache.storage.tapis;

import org.geowebcache.io.Resource;

/**
 *
 * @author pribivas
 */
public class TapisBlobStoreObject {
    
    
    private String layerName;
    private TapisBlobType type;
    private Resource resource;

    public String getLayerName() {
        return layerName;
    }

    public void setLayerName(String layerName) {
        this.layerName = layerName;
    }

    public TapisBlobType getType() {
        return type;
    }

    public void setType(TapisBlobType type) {
        this.type = type;
    }

    public Resource getResource() {
        return resource;
    }

    public void setResource(Resource resource) {
        this.resource = resource;
    }
    
}

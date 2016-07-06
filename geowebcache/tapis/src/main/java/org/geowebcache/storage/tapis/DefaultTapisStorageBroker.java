
package org.geowebcache.storage.tapis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.io.Resource;
import org.geowebcache.storage.StorageException;

/**
 *
 * @author pribivas
 */
public class DefaultTapisStorageBroker implements TapisStorageBroker {
    
    private static Log log = LogFactory.getLog(DefaultTapisStorageBroker.class);
    
    private TapisBlobStore blobStore;

    public DefaultTapisStorageBroker(TapisBlobStore blobStore) {
        this.blobStore = blobStore;
    }

    public TapisBlobStore getBlobStore() {
        return blobStore;
    }
    
    public void destroy() {
        log.info("Destroying StorageBroker");
    }
    
    public Resource get(String layerName, TapisBlobType type) throws StorageException {
        TapisBlobStoreObject tbo = new TapisBlobStoreObject();
        tbo.setLayerName(layerName);
        tbo.setType(type);
        
        return blobStore.get(tbo);
    }

    public boolean put(Resource obj, String layerName, TapisBlobType type) throws StorageException {
        TapisBlobStoreObject tbo = new TapisBlobStoreObject();
        tbo.setLayerName(layerName);
        tbo.setType(type);
        tbo.setResource(obj);
        
        return blobStore.put(tbo);
                        
    }

    public boolean delete(String layerName, TapisBlobType type) throws StorageException {
        TapisBlobStoreObject tbo = new TapisBlobStoreObject();
        tbo.setLayerName(layerName);
        tbo.setType(type);
        
        return blobStore.delete(tbo);
    }

    public boolean isExist(String layerName, TapisBlobType type) throws StorageException {
        TapisBlobStoreObject tbo = new TapisBlobStoreObject();
        tbo.setLayerName(layerName);
        tbo.setType(type);
        
        return blobStore.isExist(tbo);
    }

}

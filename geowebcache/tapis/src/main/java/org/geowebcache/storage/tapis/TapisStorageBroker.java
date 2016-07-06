package org.geowebcache.storage.tapis;

import org.geowebcache.io.Resource;
import org.geowebcache.storage.StorageException;

/**
 *
 * @author pribivas
 */
public interface TapisStorageBroker {

    public abstract Resource get(String layerName, TapisBlobType type) throws StorageException;
    public abstract boolean put(Resource obj, String layerName, TapisBlobType type) throws StorageException;
    public abstract boolean delete(String layerName, TapisBlobType type) throws StorageException;
    public abstract boolean isExist(String layerName, TapisBlobType type) throws StorageException;
    
    public abstract void destroy();
}

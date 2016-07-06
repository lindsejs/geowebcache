/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package org.geowebcache.storage.tapis;

import org.geowebcache.io.Resource;
import org.geowebcache.storage.StorageException;

/**
 *
 * @author pribivas
 */
public interface TapisBlobStore {
    
    public boolean delete(TapisBlobStoreObject storeObj) throws StorageException;
    public Resource get(TapisBlobStoreObject storeObj) throws StorageException;
    public boolean put(TapisBlobStoreObject storeObj) throws StorageException;
    public boolean isExist(TapisBlobStoreObject storeObj) throws StorageException;
    
    public void clear() throws StorageException;
    public void destroy();

}

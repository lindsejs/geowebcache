package org.geowebcache.rest.tapis;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.service.HttpErrorCodeException;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.tapis.DefaultTapisStorageBroker;
import org.geowebcache.storage.tapis.TapisBlobType;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Request;
import org.restlet.data.Response;
import org.restlet.data.Status;

/**
 *
 * @author pribivas
 */
public class GetCapabilitiesRestlet extends GWCRestlet {
    private static final Log log = LogFactory.getLog(GetCapabilitiesRestlet.class);
    
    private final TileLayerDispatcher tld;
    private final DefaultTapisStorageBroker tsb;
    
    private final TapisBlobType reqType = TapisBlobType.GET_CAPABILITIES_REQUEST;    

    public GetCapabilitiesRestlet(DefaultTapisStorageBroker tsb, TileLayerDispatcher tld) {
        this.tld = tld;
        this.tsb = tsb;
    }

    @Override
    public void handle(Request request, Response response) {
        log.debug("Handle tapis rest request: " + request.getMethod());
        Method method = request.getMethod();
        try {
            if (method.equals(Method.GET)) {
                doGet(request, response);
            } else {
                // delete xml
                if (method.equals(Method.DELETE)) {
                    doDelete(request, response);
                } else {
                    throw new RestletException("Method not allowed", Status.CLIENT_ERROR_METHOD_NOT_ALLOWED);
                }
            }
        } catch (RestletException re) {
            response.setEntity(re.getRepresentation());
            response.setStatus(re.getStatus());
        } catch (HttpErrorCodeException httpException) {
            int errorCode = httpException.getErrorCode();
            Status status = Status.valueOf(errorCode);
            response.setStatus(status);
            response.setEntity(httpException.getMessage(), MediaType.TEXT_PLAIN);
        } catch (Exception e) {
            // Either GeoWebCacheException or IOException
            response.setEntity(e.getMessage() + " " + e.toString(), MediaType.TEXT_PLAIN);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            e.printStackTrace();
        }
        
    }

    private void doGet(Request request, Response response) throws RestletException, GeoWebCacheException, StorageException {
        String layerName = (String) request.getAttributes().get("layer");
        findTileLayer(layerName, this.tld);
        
        if(!this.tsb.isExist(layerName, this.reqType)) {
            response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
        } else {
            response.setStatus(Status.SUCCESS_OK);
        }
    }

    private void doDelete(Request request, Response response) throws RestletException, GeoWebCacheException {
        String layerName = (String) request.getAttributes().get("layer");
        findTileLayer(layerName, this.tld);
        
        try {
            if(!this.tsb.isExist(layerName, this.reqType)) {
                response.setStatus(Status.CLIENT_ERROR_NOT_FOUND);
                throw new RestletException("GetCapabilities cached XML file for layer: '" + layerName + "' not found", Status.CLIENT_ERROR_NOT_FOUND);
            } else {
                // do delete
                boolean res = this.tsb.delete(layerName, this.reqType);
                if(res) {
                    response.setStatus(Status.SUCCESS_OK);
                } else {
                    throw new RestletException("GetCapabilities cached XML file for layer: '" + layerName + "' cannot delete", Status.SERVER_ERROR_INTERNAL);
                }
            }
        } catch (StorageException e) {
            response.setEntity(e.getMessage() + " " + e.toString(), MediaType.TEXT_PLAIN);
            response.setStatus(Status.SERVER_ERROR_INTERNAL);
            e.printStackTrace();
        }
    }
}

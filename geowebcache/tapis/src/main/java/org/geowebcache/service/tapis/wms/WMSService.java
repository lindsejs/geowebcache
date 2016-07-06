package org.geowebcache.service.tapis.wms;

import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.GeoWebCacheException;
import org.geowebcache.GeoWebCacheExtensions;
import org.geowebcache.TapisDispatcher;
import org.geowebcache.config.Configuration;
import org.geowebcache.config.XMLConfiguration;
import org.geowebcache.conveyor.Conveyor;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.grid.GridMismatchException;
import org.geowebcache.grid.GridSubset;
import org.geowebcache.grid.SRS;
import org.geowebcache.io.FileResource;
import org.geowebcache.io.Resource;
import org.geowebcache.layer.ProxyLayer;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.layer.wms.WMSHttpHelper;
import org.geowebcache.layer.wms.WMSLayer;
import org.geowebcache.layer.wms.WMSSourceHelper;
import org.geowebcache.mime.ImageMime;
import org.geowebcache.mime.MimeException;
import org.geowebcache.mime.MimeType;
import org.geowebcache.service.Service;
import org.geowebcache.service.ServiceException;
import org.geowebcache.service.wms.WMSUtilities;
import org.geowebcache.stats.RuntimeStats;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.tapis.DefaultTapisStorageBroker;
import org.geowebcache.storage.tapis.TapisBlobStoreObject;
import org.geowebcache.storage.tapis.TapisBlobType;
import org.geowebcache.util.NullURLMangler;
import org.geowebcache.util.ServletUtils;
import org.geowebcache.util.URLMangler;

import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.geowebcache.grid.GridUtil.findBestMatchingGrid;

/**
 *
 * @author pribivas
 */
public class WMSService extends Service {
    
    private static Log log = LogFactory.getLog(WMSService.class);
    
    public static final String SERVICE_TAPIS_WMS = "tapis_wms";
    public static final String SERVICE_WMS = "wms";
    
    static final String SERVICE_PATH = "/"+TapisDispatcher.TYPE_SERVICE+"/"+SERVICE_WMS;

    // Recombine tiles to support regular WMS clients?
    private boolean fullWMS = false;

    // Proxy requests that are not getmap or getcapabilities?
    private boolean proxyRequests = false;

    // Proxy requests that are not tiled=true?
    private boolean proxyNonTiledRequests = true;
    
    private StorageBroker sb;    
    private DefaultTapisStorageBroker tsb;
    private TileLayerDispatcher tld;
    private RuntimeStats stats;
    private URLMangler urlMangler = new NullURLMangler();
    private TapisDispatcher controller = null;   
    private String localUrl = null;

    private String protocol = null;
    private String host = null;
    private String port = null;
	
	private String hintsConfig = "DEFAULT";
    
	private WMSUtilities utility;

    /**
     * Protected no-argument constructor to allow run-time instrumentation
     */
    protected WMSService() {
        super(SERVICE_TAPIS_WMS);
    }

    public WMSService(StorageBroker sb, DefaultTapisStorageBroker tsb, TileLayerDispatcher tld, RuntimeStats stats) {
        super(SERVICE_TAPIS_WMS);

        this.sb = sb;
        this.tld = tld;
        this.stats = stats;
        this.tsb = tsb;
    }
    
    public WMSService(StorageBroker sb, DefaultTapisStorageBroker tsb, TileLayerDispatcher tld, RuntimeStats stats, URLMangler urlMangler, TapisDispatcher controller) {
        super(SERVICE_TAPIS_WMS);

        this.sb = sb;
        this.tld = tld;
        this.stats = stats;
        this.urlMangler = urlMangler;
        this.controller = controller;
        this.tsb = tsb;
    }

    private String getLocalUrl() {
        if(this.localUrl != null && !this.localUrl.isEmpty()) {
            return this.localUrl;
        }
        
        try {
            Context ctx = (Context) new InitialContext().lookup("java:comp/env");
            this.protocol = (String) ctx.lookup("tapis.geowebcache.protocol");
            this.host = (String) ctx.lookup("tapis.geowebcache.host");
            this.port = (String) ctx.lookup("tapis.geowebcache.port");
            
            if(this.host == null || "".equals(this.host)) {
                return null;
            }
            return (this.protocol != null && !"".equals(this.protocol) ? this.protocol : "http") + "://" + this.host + (this.port != null && !"".equals(this.port) ? ":" + this.port : "");
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        return this.localUrl;
    }
    
    @Override
    public ConveyorTile getConveyor(HttpServletRequest request, HttpServletResponse response)
            throws GeoWebCacheException {
        final String encoding = request.getCharacterEncoding();
        final Map requestParameterMap = request.getParameterMap();

        String[] keys = { "layers", "request", "tiled", "cached", "metatiled", "width", "height", "service", "typename" };
        Map<String, String> values = ServletUtils.selectedStringsFromMap(requestParameterMap,
                encoding, keys);

        // Look for layer
        String layers = values.get("layers");

        // Get the TileLayer
        TileLayer tileLayer = null;
        if(layers!=null) {
            tileLayer = tld.getTileLayer(layers);
        }
        // Look for requests that are not getmap
        String req = values.get("request");
        
        // Look for workspace
        String workspace = (String) request.getAttribute("workspace");
                
        // get services
        String service = values.get("service");
        
        // get typename
        String typename = values.get("typename");
        if (req != null && !req.equalsIgnoreCase("getmap")) {
            // One more chance
            if (layers == null || layers.length() == 0) {
                if(workspace != null) {
                    layers = workspace;
                } else {
                    layers = ServletUtils.stringFromMap(requestParameterMap, encoding, "layer");
                    if(layers == null || layers.length() == 0) {
                        if(service != null && "wfs".equalsIgnoreCase(service) && typename != null && !"".equals(typename)) {
                            layers = ServletUtils.stringFromMap(requestParameterMap, encoding, "typename");
                            if(layers.contains(":")) {
                                layers = layers.substring(layers.indexOf(":") + 1);
                            }
                        }
                    } 
                    values.put("LAYERS", layers);
					if(layers!=null) {
	                    tileLayer = tld.getTileLayer(layers);
	                }
                }
            }
            
			Map<String, String> filteringParameters = null;
            // If tileLayer is not null, then request parameters are extracted from it-
            if (tileLayer != null) {
                filteringParameters = tileLayer.getModifiableParameters(requestParameterMap,
                        encoding);
            }
			// Creation of a Conveyor Tile with a fake Image/png format and the associated parameters.
            ConveyorTile tile = new ConveyorTile(sb, layers, null, null,
                    ImageMime.png, filteringParameters, request, response);
            tile.setHint(req.toLowerCase());
            tile.setRequestHandler(ConveyorTile.RequestHandler.SERVICE);
            
            return tile;
        }
        
        if (layers == null) {
            throw new ServiceException("Unable to parse layers parameter from request.");
        }

        // Check whether this request is missing tiled=true
        final boolean tiled = Boolean.valueOf(values.get("tiled"));
        if (proxyNonTiledRequests && !tiled) {
            ConveyorTile tile = new ConveyorTile(sb, layers, request, response);
            tile.setHint(req);
            tile.setRequestHandler(Conveyor.RequestHandler.SERVICE);
            return tile;
        }

        String[] paramKeys = { "format", "srs", "bbox" };
        final Map<String, String> paramValues = ServletUtils.selectedStringsFromMap(
                requestParameterMap, encoding, paramKeys);

        final Map<String, String> fullParameters = tileLayer.getModifiableParameters(
                requestParameterMap, encoding);

        final MimeType mimeType;
        String format = paramValues.get("format");
        try {
            mimeType = MimeType.createFromFormat(format);
        } catch (MimeException me) {
            throw new ServiceException("Unable to determine requested format, " + format);
        }

        final SRS srs;
        {
            String requestSrs = paramValues.get("srs");
            if (requestSrs == null) {
                throw new ServiceException("No SRS specified");
            }
            srs = SRS.getSRS(requestSrs);
        }

        final BoundingBox bbox;
        {
            String requestBbox = paramValues.get("bbox");
            try {
                bbox = new BoundingBox(requestBbox);
                if (!bbox.isSane()) {
                    throw new ServiceException("The bounding box parameter (" + requestBbox
                            + ") is missing or not sane");
                }
            } catch (NumberFormatException nfe) {
                throw new ServiceException("The bounding box parameter (" + requestBbox
                        + ") is invalid");
            }
        }

        final int tileWidth = Integer.parseInt(values.get("width"));
        final int tileHeight = Integer.parseInt(values.get("height"));

        final List<GridSubset> crsMatchingSubsets = tileLayer.getGridSubsetsForSRS(srs);
        if (crsMatchingSubsets.isEmpty()) {
            throw new ServiceException("Unable to match requested SRS " + srs
                    + " to those supported by layer");
        }

        long[] tileIndexTarget = new long[3];
        GridSubset gridSubset;
        {
            GridSubset bestMatch = findBestMatchingGrid(bbox, crsMatchingSubsets, tileWidth,
                    tileHeight, tileIndexTarget);
            if (bestMatch == null) {
                // proceed as it used to be
                gridSubset = crsMatchingSubsets.get(0);
                tileIndexTarget = null;
            } else {
                gridSubset = bestMatch;
            }
        }
        
        if (fullWMS) {
            // If we support full WMS we need to do a few tests to determine whether
            // this is a request that requires us to recombine tiles to respond.
            long[] tileIndex = null;
            if (tileIndexTarget == null) {
                try {
                    tileIndex = gridSubset.closestIndex(bbox);
                } catch (GridMismatchException gme) {
                    // Do nothing, the null is info enough
                }
            } else {
                tileIndex = tileIndexTarget;
            }

            if (tileIndex == null || gridSubset.getTileWidth() != tileWidth
                    || gridSubset.getTileHeight() != tileHeight
                    || !bbox.equals(gridSubset.boundsFromIndex(tileIndex), 0.02)) {
                log.debug("Recombinining tiles to respond to WMS request");
                ConveyorTile tile = new ConveyorTile(sb, layers, request, response);
                tile.setHint("getmap");
                tile.setRequestHandler(ConveyorTile.RequestHandler.SERVICE);
                return tile;
            }
        }

        long[] tileIndex = tileIndexTarget == null ? gridSubset.closestIndex(bbox) : tileIndexTarget;

        gridSubset.checkTileDimensions(tileWidth, tileHeight);

        return new ConveyorTile(sb, layers, gridSubset.getName(), tileIndex, mimeType,
                fullParameters, request, response);
    }

    @Override
    public void handleRequest(Conveyor conv) throws GeoWebCacheException {

        log.debug("Proceeding service request, hint: " + conv.getHint());
        
        ConveyorTile tile = (ConveyorTile) conv;
        
        if (tile.getHint() != null) {
            if (tile.getHint().equalsIgnoreCase("getcapabilities")) {
                
                try {
                    final TapisBlobType reqType = TapisBlobType.GET_CAPABILITIES_REQUEST;
                    
                    // check if cached xml file is exists
                    if(tsb.isExist(tile.getLayerId(), reqType)) {
                        doProxyXmlFile(tile.servletResp, tsb.get(tile.getLayerId(), reqType), reqType);
                    } else {

                        // file not exists, get new and cache it
                        TileLayer tl = tld.getTileLayer(tile.getLayerId());
                        if(tl instanceof WMSLayer) {
                            Resource resource = doXmlResourceFile(tile, reqType);
                            if(resource != null) {
                                doProxyXmlFile(tile.servletResp, resource, reqType);
                            } else {
                                throw new GeoWebCacheException("WMS Source return empty resource!");
                            }
                            
                        } else {
                            log.error("Layer is not MSLayer, cannot cache request, do proxy instead!");
                            doInternalProxy(tile);
                        }
                    }
                } catch(GeoWebCacheException e) {
                    e.printStackTrace();
                    throw new GeoWebCacheException(e);
                } catch(StorageException e) {
                    e.printStackTrace();
                    throw new GeoWebCacheException(e);
                }
                
            } else if (tile.getHint().equalsIgnoreCase("getmap")) {
                // todo getmap
                log.debug("do getmap");
                WMSTileFuser wmsFuser = new WMSTileFuser(tld, sb, tile.servletReq);
                // Setting of the applicationContext
                wmsFuser.setApplicationContext(utility.getApplicationContext());
                // Setting of the hintConfiguration if present
                wmsFuser.setHintsConfiguration(hintsConfig);
                try {
                    wmsFuser.writeResponse(tile.servletResp, stats);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } else if (tile.getHint().equalsIgnoreCase("getfeatureinfo")) {
                // todo get feature
                log.debug("do getfeatureinfo");
                doInternalProxy(tile);
                // todo other requests with caching in files/memory
            } else {
                log.debug("do proxy");
                doInternalProxy(tile);
            }
        } else {
            throw new GeoWebCacheException("The WMS Service would love to help, "
                    + "but has no idea what you're trying to do?"
                    + "Please include request URL if you file a bug report.");
        }
    }
    
    private Resource doXmlResourceFile(ConveyorTile tile, TapisBlobType type) throws GeoWebCacheException {
        
        
        String queryStr = tile.servletReq.getQueryString();
        WMSLayer wmsLayer = (WMSLayer) tld.getTileLayer(tile.getLayerId());
        String serverStr = wmsLayer.getWMSurl()[0];
        
        GetMethod getMethod = null;
        InputStream is = null;
        Resource res = null;
        File temp = null;
        try {
            
            URL url;
            if (serverStr.contains("?")) {
                url = new URL(serverStr + "&" + queryStr);
            } else {
                url = new URL(serverStr + queryStr);
            }
            
            WMSSourceHelper helper = wmsLayer.getSourceHelper();
            if(! (helper instanceof WMSHttpHelper)) {
               throw new GeoWebCacheException("Can only proxy if WMS Layer is backed by an HTTP backend"); 
            }

            getMethod = ((WMSHttpHelper) helper).executeRequest(url, null, wmsLayer.getBackendTimeout());
            is = getMethod.getResponseBodyAsStream();

            String reqBody = IOUtils.toString(is, "UTF-8");
            IOUtils.closeQuietly(is);
            
            if(reqBody == null || "".equals(reqBody)) {
                throw new GeoWebCacheException("Response from backend is null, URL: " + url); 
            }
            
            // replace url to local
            URL serverUrl = new URL(serverStr);
            String u = serverUrl.getProtocol() + "://" + serverUrl.getHost() + (serverUrl.getPort() == -1 ? "" : ":" + serverUrl.getPort());
            
            this.localUrl = getLocalUrl();
            if(this.localUrl == null) {
                this.localUrl = tile.servletReq.getScheme() + "://" + tile.servletReq.getServerName() + (tile.servletReq.getLocalPort() == -1 ? "" : ":" + tile.servletReq.getLocalPort());
            }

            String replacedBody = reqBody.replaceAll(u, this.localUrl);
            
            // put resource
            temp = File.createTempFile(tile.getLayerId() + "_", ".tmp"); 
            FileUtils.writeStringToFile(temp, replacedBody, "UTF-8");
            res = new FileResource(temp);
            
            TapisBlobStoreObject obj = new TapisBlobStoreObject();
            obj.setLayerName(tile.getLayerId());
            obj.setResource(res);
            obj.setType(type);
            
            tsb.put(res, tile.getLayerId(), type);  
            
            // reload with TapisStorageObject Resource
            res = tsb.get(tile.getLayerId(), type);
            
        } catch (IOException ioe) {
            ioe.printStackTrace();
            log.error(ioe.getMessage());
        } finally{
            if (getMethod != null) {
                getMethod.releaseConnection();
            }
            IOUtils.closeQuietly(is);
            FileUtils.deleteQuietly(temp);
        }
        return res;
    }
    
   
    private void doProxyXmlFile(HttpServletResponse response, Resource resource, TapisBlobType type) {
        
        InputStream is = null;
        try {

            String fileName = type.name().toLowerCase() + ".xml";
            response.setStatus(HttpServletResponse.SC_OK);
            response.setContentType("text/xml"); //application/vnd.ogc.wms_xml
            response.setCharacterEncoding("UTF-8");
            response.setContentLength(Long.valueOf(resource.getSize()).intValue());
            response.setHeader("content-disposition", "inline;filename=" + fileName);
            
            int read = 0;
            byte[] data = new byte[1024];
            
            is = resource.getInputStream();
            
            while (read > -1) {
                read = is.read(data);
                if (read > -1) {
                    response.getOutputStream().write(data, 0, read);
                }
            }
            
            response.getOutputStream().flush();
        } catch (IOException ioe) {
            response.setStatus(500);
            log.error(ioe.getMessage());
        } finally{
            IOUtils.closeQuietly(is);
        }
    }
    
    
    private void doInternalProxy(ConveyorTile tile) throws GeoWebCacheException {
        // see if we can proxy the request
        TileLayer tl = tld.getTileLayer(tile.getLayerId());

        if(tl == null) {
            throw new GeoWebCacheException(tile.getLayerId() + " is unknown.");
        }

        if(tl instanceof ProxyLayer) {
            ((ProxyLayer) tl).proxyRequest(tile);
        } else {
            throw new GeoWebCacheException(tile.getLayerId() + " cannot cascade WMS requests.");
        }
    }

    /**
     * Handles a getfeatureinfo request
     *
     */
    private void handleGetFeatureInfo(ConveyorTile tile) throws GeoWebCacheException {
        TileLayer tl = tld.getTileLayer(tile.getLayerId());

        if (tl == null) {
            throw new GeoWebCacheException(tile.getLayerId() + " is unknown.");
        }

        String[] keys = { "x", "y", "srs", "info_format", "bbox", "height", "width" };
        Map<String, String> values = ServletUtils.selectedStringsFromMap(
                tile.servletReq.getParameterMap(), tile.servletReq.getCharacterEncoding(), keys);

        // TODO Arent we missing some format stuff here?
        GridSubset gridSubset = tl.getGridSubsetForSRS(SRS.getSRS(values.get("srs")));

        BoundingBox bbox = null;
        try {
            bbox = new BoundingBox(values.get("bbox"));
        } catch (NumberFormatException nfe) {
            log.debug(nfe.getMessage());
        }

        if (bbox == null || !bbox.isSane()) {
            throw new ServiceException("The bounding box parameter (" + values.get("srs")
                    + ") is missing or not sane");
        }

        // long[] tileIndex = gridSubset.closestIndex(bbox);

        MimeType mimeType;
        try {
            mimeType = MimeType.createFromFormat(values.get("info_format"));
        } catch (MimeException me) {
            throw new GeoWebCacheException("The info_format parameter ("
                    + values.get("info_format") + ")is missing or not recognized.");
        }
        
        if (mimeType != null && !tl.getInfoMimeTypes().contains(mimeType)) {
            throw new GeoWebCacheException("The info_format parameter ("
                    + values.get("info_format") + ") is not supported.");
        }

        ConveyorTile gfiConv = new ConveyorTile(sb, tl.getName(), gridSubset.getName(), null,
                mimeType, tile.getFullParameters(), tile.servletReq, tile.servletResp);
        gfiConv.setTileLayer(tl);

        int x, y;
        try {
            x = Integer.parseInt(values.get("x"));
            y = Integer.parseInt(values.get("y"));
        } catch (NumberFormatException nfe) {
            throw new GeoWebCacheException(
                    "The parameters for x and y must both be positive integers.");
        }

        int height, width;
        try {
            height = Integer.parseInt(values.get("height"));
            width = Integer.parseInt(values.get("width"));
        } catch (NumberFormatException nfe) {
            throw new GeoWebCacheException(
                    "The parameters for height and width must both be positive integers.");
        }

        Resource data = tl.getFeatureInfo(gfiConv, bbox, height, width, x, y);

        try {
            tile.servletResp.setContentType(mimeType.getMimeType());
            ServletOutputStream outputStream = tile.servletResp.getOutputStream();
            data.transferTo(Channels.newChannel(outputStream));
            outputStream.flush();
        } catch (IOException ioe) {
            tile.servletResp.setStatus(500);
            log.error(ioe.getMessage());
        }

    }

    public void setFullWMS(String trueFalse) {
        // Selection of the configurations 
        List<Configuration> configs = new ArrayList<Configuration>(GeoWebCacheExtensions.extensions(Configuration.class));
        // Selection of the Configuration file associated to geowebcache.xml
        XMLConfiguration gwcXMLconfig = null;
        for(Configuration config : configs){
            if(config instanceof XMLConfiguration){
                gwcXMLconfig = (XMLConfiguration) config;
                break;
            }
        }
        // From the configuration file the "fullWMS" parameter is searched
        Boolean wmsFull = null;
        if(gwcXMLconfig!=null){
            wmsFull = gwcXMLconfig.getfullWMS();
        }                
        
        if(wmsFull!=null){
            this.fullWMS = wmsFull;
        }else{
            this.fullWMS = Boolean.parseBoolean(trueFalse);            
        }
        // Log if fullWMS is enabled
        if (this.fullWMS) {
            log.info("Will recombine tiles for non-tiling clients.");
        } else {
            log.info("Will NOT recombine tiles for non-tiling clients.");
        }
    }

    public void setProxyRequests(String trueFalse) {
        this.proxyRequests = Boolean.parseBoolean(trueFalse);
        if (this.proxyRequests) {
            log.info("Will proxy requests to backend that are not getmap or getcapabilities.");
        } else {
            log.info("Will NOT proxy non-getMap requests to backend.");
        }
    }

    public void setProxyNonTiledRequests(String trueFalse) {
        this.proxyNonTiledRequests = Boolean.parseBoolean(trueFalse);
        if (this.proxyNonTiledRequests) {
            log.info("Will proxy requests that miss tiled=true to backend.");
        } else {
            log.info("Will NOT proxy requests that miss tiled=true to backend.");
        }
    }

    public void setHintsConfig(String hintsConfig) {
        this.hintsConfig = hintsConfig;
    }
    
    public void setUtility(WMSUtilities utility) {
        this.utility = utility;
    }
}

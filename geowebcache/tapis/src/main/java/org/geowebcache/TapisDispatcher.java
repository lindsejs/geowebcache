package org.geowebcache;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.httpclient.util.DateParseException;
import org.apache.commons.httpclient.util.DateUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.config.Configuration;
import org.geowebcache.conveyor.Conveyor;
import org.geowebcache.conveyor.ConveyorTile;
import org.geowebcache.filter.request.RequestFilterException;
import org.geowebcache.grid.BoundingBox;
import org.geowebcache.grid.GridSubset;
import org.geowebcache.grid.OutsideCoverageException;
import org.geowebcache.io.ByteArrayResource;
import org.geowebcache.io.Resource;
import org.geowebcache.layer.BadTileException;
import org.geowebcache.layer.TileLayer;
import org.geowebcache.layer.TileLayerDispatcher;
import org.geowebcache.mime.ImageMime;
import org.geowebcache.service.HttpErrorCodeException;
import org.geowebcache.service.OWSException;
import org.geowebcache.service.Service;
import org.geowebcache.stats.RuntimeStats;
import org.geowebcache.storage.DefaultStorageFinder;
import org.geowebcache.storage.StorageBroker;
import org.geowebcache.util.GWCVars;
import org.geowebcache.util.ServletUtils;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.mvc.AbstractController;

/**
 *
 * @author pribivas
 */
public class TapisDispatcher extends AbstractController {

    private static final Log log = LogFactory.getLog(TapisDispatcher.class);

    public static final String TYPE_SERVICE = "tapis";

    private TileLayerDispatcher tileLayerDispatcher = null;
    private DefaultStorageFinder defaultStorageFinder = null;
    private StorageBroker storageBroker;
    private RuntimeStats runtimeStats;
    private Map<String, Service> services = null;
    private Resource blankTile = null;
    private String servletPrefix = null;
    
    private boolean mustRevalidate = false;

    /**
     * Should be invoked through Spring
     *
     * @param tileLayerDispatcher
     * @param storageBroker
     * @param mainConfiguration
     * @param runtimeStats
     */
    public TapisDispatcher(TileLayerDispatcher tileLayerDispatcher, StorageBroker storageBroker, Configuration mainConfiguration, RuntimeStats runtimeStats) {
        super();
        this.tileLayerDispatcher = tileLayerDispatcher;
        this.runtimeStats = runtimeStats;
        this.storageBroker = storageBroker;

        // get from JNDI config mustrevalidate param
        this.mustRevalidate = getMustRevalidate();
        
        if (mainConfiguration.isRuntimeStatsEnabled()) {
            this.runtimeStats.start();
        } else {
            runtimeStats = null;
        }
    }
    
    private boolean getMustRevalidate() {
                
        try {
            Context ctx = (Context) new InitialContext().lookup("java:comp/env");
            return Boolean.valueOf((String) ctx.lookup("tapis.geowebcache.header.must.revalidate"));
        } catch(Exception e) {
            e.printStackTrace();
        }
        
        return false;
    }

    public void setStorageBroker() {
        // This is just to force initialization
        log.debug("TapisDispatcher received StorageBroker : " + storageBroker.toString());
    }

    public void setDefaultStorageFinder(DefaultStorageFinder defaultStorageFinder) {
        this.defaultStorageFinder = defaultStorageFinder;
    }

    /**
     * GeoServer and other solutions that embedded this dispatcher will prepend a path, this is used to remove it.
     *
     * @param servletPrefix
     */
    public void setServletPrefix(String servletPrefix) {
        if (!servletPrefix.startsWith("/")) {
            this.servletPrefix = "/" + servletPrefix;
        } else {
            this.servletPrefix = servletPrefix;
        }

        log.info("Invoked setServletPrefix(" + servletPrefix + ")");
    }

    /**
     * GeoServer and other solutions that embedded this dispatcher will prepend a path, this is used to remove it.
     */
    public String getServletPrefix() {
        return servletPrefix;
    }

    /**
     * Services convert HTTP requests into the internal grid representation and specify what layer the response should come from.
     *
     * The application context is scanned for objects extending Service, thereby making it easy to add new services.
     *
     * @return
     */
    private Map<String, Service> loadServices() {
        log.info("Loading GWC Service extensions...");

        List<Service> plugins = GeoWebCacheExtensions.extensions(Service.class);
        Map<String, Service> _services = new HashMap<String, Service>();
        // Give all service objects direct access to the tileLayerDispatcher
        for (Service aService : plugins) {
            _services.put(aService.getPathName(), aService);
        }
        log.info("Done loading GWC Service extensions. Found : "
                        + new ArrayList<String>(_services.keySet()));
        return _services;
    }

    private void loadBlankTile() {
        String blankTilePath = defaultStorageFinder
                        .findEnvVar(DefaultStorageFinder.GWC_BLANK_TILE_PATH);

        if (blankTilePath != null) {
            File fh = new File(blankTilePath);
            if (fh.exists() && fh.canRead() && fh.isFile()) {
                long fileSize = fh.length();
                blankTile = new ByteArrayResource(new byte[(int) fileSize]);
                try {
                    loadBlankTile(blankTile, fh.toURI().toURL());
                } catch (IOException e) {
                    e.printStackTrace();
                }

                if (fileSize == blankTile.getSize()) {
                    log.info("Loaded blank tile from " + blankTilePath);
                } else {
                    log.error("Failed to load blank tile from " + blankTilePath);
                }

                return;
            } else {
                log.error("" + blankTilePath + " does not exist or is not readable.");
            }
        }

        // Use the built-in one:
        try {
            URL url = GeoWebCacheDispatcher.class.getResource("blank.png");
            blankTile = new ByteArrayResource();
            loadBlankTile(blankTile, url);
            int ret = (int) blankTile.getSize();
            log.info("Read " + ret + " from blank PNG file (expected 425).");
        } catch (IOException ioe) {
            log.error(ioe.getMessage());
        }
    }

    private void loadBlankTile(Resource blankTile, URL source) throws IOException {
        InputStream inputStream = source.openStream();
        ReadableByteChannel ch = Channels.newChannel(inputStream);
        try {
            blankTile.transferFrom(ch);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            ch.close();
        }
    }

    /**
     * Spring function for MVC, this is the entry point for the application.
     *
     * If a tile is requested the request will be handed off to handleServiceRequest.
     *
     * @param request
     * @param response
     * @return
     * @throws java.lang.Exception
     */
    @Override
    protected ModelAndView handleRequestInternal(HttpServletRequest request, HttpServletResponse response) throws Exception {

        log.debug("Proceeding internal request: " + request.getRequestURI());
        
        // Break the request into components, {type, service name}
        String[] requestComps = null;
        String normalizedURI = null;
        try {
            normalizedURI = request.getRequestURI().replaceFirst(request.getContextPath(), "");

            if (servletPrefix != null) {
                normalizedURI = normalizedURI.replaceFirst(servletPrefix, ""); // getRequestURI().replaceFirst(request.getContextPath()+, ");
            }
            log.debug("Normalized url: " + normalizedURI);
            requestComps = parseRequest(normalizedURI);           
        } catch (GeoWebCacheException gwce) {
            writeError(response, 400, gwce.getMessage());
            return null;
        }

        try {
            if (requestComps[0].equalsIgnoreCase(TYPE_SERVICE)) {
                String servName = requestComps[1];
                String workspace = null;
                if("wms".equals(requestComps[1]) || "wmts".equals(requestComps[1])) {
                    servName = "tapis_" + requestComps[1];
                } else if("wms".equals(requestComps[2]) || "wmts".equals(requestComps[2])) {
                    // with workspace
                    servName = "tapis_" + requestComps[2];
                    workspace = requestComps[1];
                    log.debug("Found request with workspace: " + workspace);
                }
                handleServiceRequest(servName, normalizedURI, workspace, request, response);
            } else {
                writeError(response, 404, "Unknown path: " + requestComps[0]);
            }
        } catch (HttpErrorCodeException e) {
            writeFixedResponse(response, e.getErrorCode(), "text/plain", new ByteArrayResource(e
                            .getMessage().getBytes()), Conveyor.CacheResult.OTHER);
        } catch (RequestFilterException e) {

            RequestFilterException reqE = (RequestFilterException) e;
            reqE.setHttpInfoHeader(response);

            writeFixedResponse(response, reqE.getResponseCode(), reqE.getContentType(),
                            reqE.getResponse(), Conveyor.CacheResult.OTHER);
        } catch (OWSException e) {
            OWSException owsE = (OWSException) e;
            writeFixedResponse(response, owsE.getResponseCode(), owsE.getContentType(),
                            owsE.getResponse(), Conveyor.CacheResult.OTHER);
        } catch (Exception e) {
            e.printStackTrace();
            if (!(e instanceof BadTileException) || log.isDebugEnabled()) {
                log.error(e.getMessage() + " " + request.getRequestURL().toString());
            }

            writeError(response, 400, e.getMessage());

            if (!(e instanceof GeoWebCacheException) || log.isDebugEnabled()) {
                e.printStackTrace();
            }
        }
        return null;
    }

    /**
     * Destroy function, has to be referenced in bean declaration: <bean ... destroy="destroy">...</bean>
     */
    public void destroy() {
        log.info("TapisDispatcher.destroy() was invoked, shutting down.");
    }

    /**
     * Essentially this slices away the prefix, leaving type and request
     *
     * @param servletPath
     * @return {type, service}ervletPrefix
     */
    private String[] parseRequest(String servletPath) throws GeoWebCacheException {
        String[] retStrs = new String[3];
        String[] splitStr = servletPath.split("/");

        if (splitStr == null || splitStr.length < 2) {
            return null;
        }

        retStrs[0] = splitStr[1];
        retStrs[1] = splitStr[2];
        if (splitStr.length > 3) {
            retStrs[2] = splitStr[3];
        }
        
        return retStrs;
    }

    /**
     * This is the main method for handling service requests. See comments in the code.
     *
     * @param request
     * @param response
     * @throws Exception
     */
    private void handleServiceRequest(String serviceStr, String normalizedURI, String workspace, HttpServletRequest request,
                    HttpServletResponse response) throws Exception {

        log.debug("Handle service request: " + serviceStr);
        Conveyor conv = null;

        // 1) Figure out what Service should handle this request
        Service service = findService(serviceStr);

        // if has workspace, add it to request map
        if(workspace != null) {
            request.setAttribute("workspace", workspace);
        }
        
        if(normalizedURI != null) {
            request.setAttribute("normalizeduri", normalizedURI);
        }
        
        // 2) Find out what layer will be used and how
        conv = service.getConveyor(request, response);
        final String layerName = conv.getLayerId();
        log.debug("Start rendering layer: " + layerName + " on service: " + service.getPathName() + " reqHandler: " + conv.getRequestHandler() + " hint: " + conv.getHint());
        if(layerName == null || (layerName != null && tileLayerDispatcher.getTileLayer(layerName) == null)) {
            if("tapis_wms".equals(serviceStr)) {
                conv.setHint("nocache");
                conv.setRequestHandler(Conveyor.RequestHandler.SERVICE);
            }
        } else {
            if (layerName != null && !tileLayerDispatcher.getTileLayer(layerName).isEnabled()) {
                throw new OWSException(400, "InvalidParameterValue", "LAYERS", "Layer '" + layerName
                                + "' is disabled");
            }
        }

        // Check where this should be dispatched
        if (conv.reqHandler == Conveyor.RequestHandler.SERVICE) {
            // A3 The service object takes it from here
            service.handleRequest(conv);

        } else {
            ConveyorTile convTile = (ConveyorTile) conv;

            // B3) Get the configuration that has to respond to this request
            TileLayer layer = tileLayerDispatcher.getTileLayer(layerName);

            // Save it for later
            convTile.setTileLayer(layer);

            // Apply the filters
            layer.applyRequestFilters(convTile);

            // Keep the URI
            // tile.requestURI = request.getRequestURI();
            try {
                // A5) Ask the layer to provide the content for the tile
                convTile = layer.getTile(convTile);

                // A6) Write response
                writeData(convTile);

                // Alternatively:
            } catch (OutsideCoverageException e) {
                writeEmpty(convTile, e.getMessage());
            }
        }
    }

    /**
     * Helper function for looking up the service that should handle the request.
     *
     * @param request full HttpServletRequest
     * @return
     */
    private Service findService(String serviceStr) throws GeoWebCacheException {
        if (this.services == null) {
            this.services = loadServices();
            loadBlankTile();
        }

        // E.g. /wms/test -> /wms
        Service service = (Service) services.get(serviceStr);
        if (service == null) {
            if (serviceStr == null || serviceStr.length() == 0) {
                serviceStr = ", try service/&lt;name of service&gt;";
            } else {
                serviceStr = " \"" + serviceStr + "\"";
            }
            throw new GeoWebCacheException("Unable to find handler for service" + serviceStr);
        }
        return service;
    }

    /**
     * Wrapper method for writing an error back to the client, and logging it at the same time.
     *
     * @param response where to write to
     * @param httpCode the HTTP code to provide
     * @param errorMsg the actual error message, human readable
     */
    private void writeError(HttpServletResponse response, int httpCode, String errorMsg) {
        log.debug(errorMsg);

        errorMsg = "<html> " + TapisDispatcher.gwcHtmlHeader("GWC Error") + "<body> "
                        + "<h4>" + httpCode + ": "
                        + ServletUtils.disableHTMLTags(errorMsg) + "</h4>" + "</body></html> ";
        writePage(response, httpCode, errorMsg);
    }

    private void writePage(HttpServletResponse response, int httpCode, String message) {
        Resource res = new ByteArrayResource(message.getBytes());
        writeFixedResponse(response, httpCode, "text/html", res, Conveyor.CacheResult.OTHER);
    }
    
    private static String gwcHtmlHeader(String pageTitle) {
        return "<head> " + "<title>" + pageTitle + "</title>" + "<style type=\"text/css\"> "
                + "body, td { "
                + "font-family: Verdana,Arial,\'Bitstream Vera Sans\',Helvetica,sans-serif; "
                + "font-size: 0.85em; " + "vertical-align: top; " + "} " + "</style> "
                + "</head> ";
    }

    
    public void setExpirationHeader(TileLayer layer, HttpServletResponse response, int zoomLevel) {
        
        // check from config is revalidation
        if(!this.mustRevalidate) {
        
            int expireValue = layer.getExpireClients(zoomLevel);

            // Fixed value
            if (expireValue == GWCVars.CACHE_VALUE_UNSET) {
                return;
            }

            // without must revalidate
            if (expireValue > 0) {
                response.setHeader("Cache-Control", "max-age=" + expireValue);
                response.setHeader("Expires", ServletUtils.makeExpiresHeader(expireValue));
            } else if (expireValue == GWCVars.CACHE_NEVER_EXPIRE) {
                long oneYear = 3600 * 24 * 365;
                response.setHeader("Cache-Control", "max-age=" + oneYear);
                response.setHeader("Expires", ServletUtils.makeExpiresHeader((int) oneYear));
            } else if (expireValue == GWCVars.CACHE_DISABLE_CACHE) {
                response.setHeader("Cache-Control", "no-cache");
            } else if (expireValue == GWCVars.CACHE_USE_WMS_BACKEND_VALUE) {
                int seconds = 3600;
                response.setHeader("geowebcache-error", "No real CacheControl information available");
                response.setHeader("Cache-Control", "max-age=" + seconds);
                response.setHeader("Expires", ServletUtils.makeExpiresHeader(seconds));
            }
        
        }
    }
    
    /**
     * Happy ending, sets the headers and writes the response back to the client.
     */
    private void writeData(ConveyorTile tile) throws IOException {
        HttpServletResponse servletResp = tile.servletResp;
        final HttpServletRequest servletReq = tile.servletReq;

        final Conveyor.CacheResult cacheResult = tile.getCacheResult();
        int httpCode = HttpServletResponse.SC_OK;
        String mimeType = tile.getMimeType().getMimeType();
        Resource blob = tile.getBlob();
        
        servletResp.setHeader("geowebcache-cache-result", String.valueOf(cacheResult));
        servletResp.setHeader("geowebcache-tile-index", Arrays.toString(tile.getTileIndex()));
        long[] tileIndex = tile.getTileIndex();
        TileLayer layer = tile.getLayer();
        GridSubset gridSubset = layer.getGridSubset(tile.getGridSetId());
        BoundingBox tileBounds = gridSubset.boundsFromIndex(tileIndex);
        servletResp.setHeader("geowebcache-tile-bounds", tileBounds.toString());
        servletResp.setHeader("geowebcache-gridset", gridSubset.getName());
        servletResp.setHeader("geowebcache-crs", gridSubset.getSRS().toString());
        
        //set expire cache control
        setExpirationHeader(layer, servletResp, (int) tile.getTileIndex()[2]);

        final long tileTimeStamp = tile.getTSCreated();
        final String ifModSinceHeader = servletReq.getHeader("If-Modified-Since");
        // commons-httpclient's DateUtil can encode and decode timestamps formatted as per RFC-1123,
        // which is one of the three formats allowed for Last-Modified and If-Modified-Since headers
        // (e.g. 'Sun, 06 Nov 1994 08:49:37 GMT'). See
        // http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.3.1

        final String lastModified = org.apache.commons.httpclient.util.DateUtil
                        .formatDate(new Date(tileTimeStamp));
        servletResp.setHeader("Last-Modified", lastModified);

        final Date ifModifiedSince;
        if (ifModSinceHeader != null && ifModSinceHeader.length() > 0) {
            try {
                ifModifiedSince = DateUtil.parseDate(ifModSinceHeader);
                // the HTTP header has second precision
                long ifModSinceSeconds = 1000 * (ifModifiedSince.getTime() / 1000);
                long tileTimeStampSeconds = 1000 * (tileTimeStamp / 1000);
                if (ifModSinceSeconds >= tileTimeStampSeconds) {
                    httpCode = HttpServletResponse.SC_NOT_MODIFIED;
                    blob = null;
                }
            } catch (DateParseException e) {
                if (log.isDebugEnabled()) {
                    log.debug("Can't parse client's If-Modified-Since header: '" + ifModSinceHeader
                                    + "'");
                }
            }
        }

        if (httpCode == HttpServletResponse.SC_OK && tile.getLayer().useETags()) {
            String ifNoneMatch = servletReq.getHeader("If-None-Match");
            String hexTag = Long.toHexString(tileTimeStamp);

            if (ifNoneMatch != null) {
                if (ifNoneMatch.equals(hexTag)) {
                    httpCode = HttpServletResponse.SC_NOT_MODIFIED;
                    blob = null;
                }
            }

            // If we get here, we want ETags but the client did not have the tile.
            servletResp.setHeader("ETag", hexTag);
        }

        int contentLength = (int) (blob == null ? -1 : blob.getSize());
        writeFixedResponse(servletResp, httpCode, mimeType, blob, cacheResult, contentLength);
    }

    /**
     * Writes a transparent, 8 bit PNG to avoid having clients like OpenLayers showing lots of pink tiles
     */
    private void writeEmpty(ConveyorTile tile, String message) {
        tile.servletResp.setHeader("geowebcache-message", message);
        TileLayer layer = tile.getLayer();
        if (layer != null) {
            layer.setExpirationHeader(tile.servletResp, (int) tile.getTileIndex()[2]);

            if (layer.useETags()) {
                String ifNoneMatch = tile.servletReq.getHeader("If-None-Match");
                if (ifNoneMatch != null && ifNoneMatch.equals("gwc-blank-tile")) {
                    tile.servletResp.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                    return;
                } else {
                    tile.servletResp.setHeader("ETag", "gwc-blank-tile");
                }
            }
        }

        writeFixedResponse(tile.servletResp, 200, ImageMime.png.getMimeType(), this.blankTile,
                        Conveyor.CacheResult.OTHER);
    }

    private void writeFixedResponse(HttpServletResponse response, int httpCode, String contentType,
                    Resource resource, Conveyor.CacheResult cacheRes) {

        int contentLength = (int) (resource == null ? -1 : resource.getSize());
        writeFixedResponse(response, httpCode, contentType, resource, cacheRes, contentLength);
    }

    private void writeFixedResponse(HttpServletResponse response, int httpCode, String contentType,
                    Resource resource, Conveyor.CacheResult cacheRes, int contentLength) {

        response.setStatus(httpCode);
        response.setContentType(contentType);

        response.setContentLength((int) contentLength);
        if (resource != null) {
            try {
                OutputStream os = response.getOutputStream();
                resource.transferTo(Channels.newChannel(os));

                runtimeStats.log(contentLength, cacheRes);

            } catch (IOException ioe) {
                log.debug("Caught IOException: " + ioe.getMessage() + "\n " + ioe.toString());
            }
        }
    }
}

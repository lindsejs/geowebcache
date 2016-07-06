

package org.geowebcache.storage.tapis;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.geowebcache.config.ConfigurationException;
import org.geowebcache.io.FileResource;
import org.geowebcache.io.Resource;
import org.geowebcache.storage.BlobStoreListener;
import org.geowebcache.storage.DefaultStorageFinder;
import org.geowebcache.storage.StorageException;
import org.geowebcache.storage.blobstore.file.FilePathGenerator;
import static org.geowebcache.storage.blobstore.file.FilePathUtils.filteredLayerName;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;

/**
 *
 * @author pribivas
 */
public class FileTapisBlobStore implements TapisBlobStore, BlobStoreListener {
    
    private static Log log = LogFactory.getLog(FileTapisBlobStore.class);

    public static final int BUFFER_SIZE = 32768;

    private final File stagingArea;

    private final String path;

    private FilePathGenerator pathGenerator;

    private File tmp;
    
    private final String EXT = ".xml";

    private static ExecutorService deleteExecutorService;

    public FileTapisBlobStore(DefaultStorageFinder defStoreFinder) throws StorageException, ConfigurationException {
        this(defStoreFinder.getDefaultPath());
    }

    public FileTapisBlobStore(String rootPath) throws StorageException {
        this.path = rootPath;
        this.pathGenerator = new FilePathGenerator(this.path);

        // prepare the root
        File fh = new File(path);
        fh.mkdirs();
        if (!fh.exists() || !fh.isDirectory() || !fh.canWrite()) {
            throw new StorageException(path + " is not writable directory.");
        }
        
        // and the temporary directory
        tmp = new File(path, "tmp");
        tmp.mkdirs();
        if (!tmp.exists() || !tmp.isDirectory() || !tmp.canWrite()) {
            throw new StorageException(tmp.getPath() + " is not writable directory.");
        }
        
        stagingArea = new File(path, "_gwc_in_progress_deletes_");
        createDeleteExecutorService();
        issuePendingDeletes();
    }

    private void issuePendingDeletes() {
        if (!stagingArea.exists()) {
            return;
        }
        if (!stagingArea.isDirectory() || !stagingArea.canWrite()) {
            throw new IllegalStateException("Staging area is not writable or is not a directory: "
                    + stagingArea.getAbsolutePath());
        }
        File[] pendings = stagingArea.listFiles();
        for (File directory : pendings) {
            if (directory.isDirectory()) {
                deletePending(directory);
            }
        }
    }

    private void deletePending(final File pendingDeleteDirectory) {
        FileTapisBlobStore.deleteExecutorService.submit(new DefferredDirectoryDeleteTask(
                pendingDeleteDirectory));
    }

    private void createDeleteExecutorService() {
        CustomizableThreadFactory tf;
        tf = new CustomizableThreadFactory("GWC FileStore delete directory thread-");
        tf.setDaemon(true);
        tf.setThreadPriority(Thread.MIN_PRIORITY);
        deleteExecutorService = Executors.newFixedThreadPool(1);
    }

    /**
     * Destroy method for Spring
     */
    public void destroy() {
        deleteExecutorService.shutdownNow();
    }

    private static class DefferredDirectoryDeleteTask implements Runnable {

        private final File directory;

        public DefferredDirectoryDeleteTask(final File directory) {
            this.directory = directory;
        }

        public void run() {
            try {
                deleteDirectory(directory);
            } catch (IOException e) {
                log.warn("Exception occurred while deleting '" + directory.getAbsolutePath() + "'",
                        e);
            } catch (InterruptedException e) {
                log.info("FileStore delete background service interrupted while deleting '"
                        + directory.getAbsolutePath()
                        + "'. Process will be resumed at next start up");
            }
        }

        private void deleteDirectory(File directory) throws IOException, InterruptedException {
            if (!directory.exists()) {
                return;
            }
            if (Thread.interrupted()) {
                throw new InterruptedException();
            }
            File[] files = directory.listFiles();
            for (int i = 0; i < files.length; i++) {
                if (Thread.interrupted()) {
                    throw new InterruptedException();
                }
                File file = files[i];
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    if (!file.delete()) {
                        throw new IOException("Unable to delete " + file.getAbsolutePath());
                    }
                }
            }
            if (!directory.delete()) {
                String message = "Unable to delete directory " + directory + ".";
                throw new IOException(message);
            }
        }

    }
    
    private File getLayerPath(String layerName) {
        String prefix = path + File.separator + filteredLayerName(layerName);

        File layerPath = new File(prefix);
        return layerPath;
    }
    
    private String getFileName(TapisBlobStoreObject storeObj) {
        return storeObj.getType().name().toLowerCase() + EXT;
    }
        
    // implementation
    public boolean delete(TapisBlobStoreObject storeObj) throws StorageException {
        File sourcePath = getLayerPath(storeObj.getLayerName());
        String fileName = getFileName(storeObj);
        File file = new File(sourcePath, fileName);
        
        log.debug("Try to delete file: " + file.getAbsolutePath());
        if(file.exists()) {
            return file.delete();
        }
        
        return false;
    }

    public Resource get(TapisBlobStoreObject storeObj) throws StorageException {
        File sourcePath = getLayerPath(storeObj.getLayerName());
        String fileName = getFileName(storeObj);
        File file = new File(sourcePath, fileName);
        
        log.debug("Try to return file: " + file.getAbsolutePath());
        return new FileResource(file);
        
    }
    
    public boolean isExist(TapisBlobStoreObject storeObj) throws StorageException {
        File sourcePath = getLayerPath(storeObj.getLayerName());
        String fileName = getFileName(storeObj);
        File file = new File(sourcePath, fileName);

        return file.exists() && file.canRead();
    }
    
    public boolean put(TapisBlobStoreObject storeObj) throws StorageException {
        File sourcePath = getLayerPath(storeObj.getLayerName());
        String fileName = getFileName(storeObj);
        File file = new File(sourcePath, fileName);
        if(!file.getParentFile().exists() && !file.getParentFile().mkdirs()) {
            throw new StorageException("Cannot create " + file.getParentFile().getAbsolutePath() + " path");
        }
        try {
            IOUtils.copy(storeObj.getResource().getInputStream(), new FileOutputStream(file));
        } catch(IOException e) {
            throw new StorageException(e.getLocalizedMessage());
        }
        
        return false;
    }

    public void clear() throws StorageException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void tileStored(String layerName, String gridSetId, String blobFormat, String parametersId, long x, long y, int z, long blobSize) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void tileDeleted(String layerName, String gridSetId, String blobFormat, String parametersId, long x, long y, int z, long blobSize) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void tileUpdated(String layerName, String gridSetId, String blobFormat, String parametersId, long x, long y, int z, long blobSize, long oldSize) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void layerDeleted(String layerName) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void layerRenamed(String oldLayerName, String newLayerName) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public void gridSubsetDeleted(String layerName, String gridSetId) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
    
}

package TaggingApplication;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import com.azure.core.http.rest.PagedIterable;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.*;

/** Represents azure storage service
 * @author Charlene Pang
 */

public class StorageService {
    private BlobServiceClient blobServiceClient;
    private String connectStr;

    /** Constructor, initialises connection string and serviceClient to perform actions on blobs
     * @param connectStr Connection String to Azure storage account
     */
    public StorageService(String connectStr)
    {
        this.blobServiceClient = new BlobServiceClientBuilder().connectionString(connectStr).buildClient();
        this.connectStr = connectStr;
    }

    /** Uploads a single file to container from Google Drive/local device
     * @param path String that represents local path of file
     * @param container String that represents destination container's name
     * @param tags Map that stores specified tagged values for file
     * @param outputTypes List that stores all output types in container
     */
    private void uploadFiles(String path ,String container, Map<String,String> tags)
    {
        BlobContainerClient containerClient = new BlobContainerClientBuilder()
        .connectionString(connectStr).containerName(container).buildClient();
        System.out.println("Uploading files to storage");

        //creates container if not exist
        if (!containerClient.exists()){
            containerClient = createContainer(container);
        }

        //parsing path string to obtain document type
        String extension = path.substring(path.lastIndexOf('.') + 1).toLowerCase();
        String fileName = path.substring(path.lastIndexOf('/')+1);
        String truncate = path.substring(0, path.lastIndexOf('/'));
        String docType = truncate.substring(truncate.lastIndexOf('/')+1);

        try
        {
            HashMap<String,String> newtags = new HashMap<>();
            newtags.putAll(tags);

            //categorising files into docTypes (ie pdf, png, ppt)
            BlobClient blobClient = containerClient.getBlobClient(String.format("%s/%s",extension,fileName));
            blobClient.uploadFromFile(path);

            //automatically assign tags if not assigned yet
            if (tags.get("DocType").equals("TBA"))
            {
                newtags.replace("DocType", docType);
            }

            if (fileName.contains("redacted"))
            {
                newtags.replace("Redacted","True");
            }
            blobClient.setTags(newtags);
            System.out.println(String.format("%s uploaded ",fileName));
        }

        catch (Exception e)
        {
            System.out.println(e);
        }
    }

    /** Uploads a folder to a destination container from Google Drive/local device
     * @param path String representing local path of folder
     * @param container String representing destination container's name
     * @param tags Map of tag values to be applied on all documents in folder
     */
    public void uploadFolder(String path,String container, Map<String,String> tags,List<String> outputTypes)
    {  
        //get all files/folders in path
        List<Path> files = getFiles(path);
        String projectName = path.substring(path.lastIndexOf('/')+ 1);
        List<Path> outputPaths = new ArrayList<>();

        //automatically assign Project Name if it is TBA
        if (tags.get("Project Name").equals("TBA"))
        {
            tags.replace("Project Name", projectName);
        }
        for (Path p : files)
        {
            String extension = p.toString().substring(p.toString().lastIndexOf('.') + 1).toLowerCase();
            
            if (outputTypes.contains(extension)){
                outputPaths.add(p);
                continue;
            }
            //if there are unlinked documents within folder, project name is parent folder
            if (Files.isRegularFile(p))
            {
                uploadFiles(p.toFile().toString(), container, tags);
            }
            else
            {
                uploadFolder(p.toFile().getPath(),container,tags,outputTypes);
                System.out.println("folder has other folder");
            }       
        }

        //copying tags from input file to corresponding output file
        for (Path p : outputPaths){
            //parsing file names
            String fileName = p.toString().substring(0,p.toString().lastIndexOf("."));
            String truncate = fileName.substring(fileName.lastIndexOf("/"));
            String extension = truncate.substring(truncate.lastIndexOf(".")+1);
            String inputBlobName = String.format("%s%s",extension,truncate);
            String outputBlobName = p.toString().substring(p.toString().lastIndexOf("/"));
            String outputBlobExtension = outputBlobName.substring(outputBlobName.lastIndexOf(".")+1);
            String blobName = String.format("%s%s",outputBlobExtension,outputBlobName);

            BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
            BlobClient inputBlobClient = containerClient.getBlobClient(inputBlobName);
            BlobClient outputBlobClient = containerClient.getBlobClient(blobName);
            outputBlobClient.uploadFromFile(p.toFile().toString());
            outputBlobClient.setTags(inputBlobClient.getTags());
        }
    }

    /** Gets the files from specified location on Google Drive/local device
     * @param sourceFolder String representing local path of folder
     * @return List of paths of all files within sourceFolder
     */
    private List<Path> getFiles(String sourceFolder)
    {
        Path path = Paths.get(sourceFolder);

        try{
            //add all files without hidden attributes 
            Stream<Path> subPaths = Files.walk(path);
            List<Path> fileList = subPaths.filter(a -> !a.toFile().isHidden() 
                && Files.isRegularFile(a)).collect(Collectors.toList());
            subPaths.close();
            return fileList;
        }
        catch (Exception e)
        {
            System.out.println(e);
        }
        return null;
    }

    /** Finds files with specified document type within specified container
     * @param value String representing document type to be found
     * @param containerName String representing container to be searched
     * @return List of blobs with specified document type within container
     */
    public List<BlobItem> findDocType(String value, String containerName)
    {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(containerName);
        List<BlobItem> blobs = containerClient.listBlobs().stream().collect(Collectors.toList());
        List<BlobItem> foundItems = new ArrayList<>();

        for (BlobItem blobItem : blobs)
        {
            BlobClient blobClient = containerClient.getBlobClient(blobItem.getName());
            Map<String,String> dic = blobClient.getTags(); 

            if (dic.get("DocType") != null && dic.get("DocType").equals(value)){
                foundItems.add(blobItem);    
            }
        }
        System.out.println("All matching documents in blob found ");
        foundItems.forEach(a -> System.out.println(a.getName()));
        return foundItems;
    }

    /**
     * Finds blobs with particular document type for all blobs in a container and
     * moves blobs to specified container for organising purposes
     * @param value String representing document type to be moved
     * @param destContainerName String representing container name folders should be moved to
     * @return List of blobs found within storage account with specified document type
     */
    public List<BlobItem> regroup(String value,String destContainerName,String sourceContainerName)
    {
        BlobContainerClient destContainerClient = blobServiceClient
            .getBlobContainerClient(destContainerName);
        List<BlobItem> foundItems = new ArrayList<BlobItem>();
        BlobContainerClient containerClient = blobServiceClient
            .getBlobContainerClient(sourceContainerName);
        System.out.println("in container " + sourceContainerName);
        List<BlobItem> blobs = findDocType(value, sourceContainerName);
        for (BlobItem blob : blobs)
        {
            if (blob != null)
                foundItems.add(blob);
                BlobClient destblobClient = destContainerClient.getBlobClient(blob.getName());
                BlobClient startblobClient = containerClient.getBlobClient(blob.getName());
                destblobClient.beginCopy(startblobClient.getBlobUrl(), null);
                destblobClient.setTags(startblobClient.getTags());                  
            
        }
        return foundItems;
    }

    /** Migrates files from path provided to different containers based on the folder name 
     * that file is stored in.
     * @param path String representing path to files on local desktop
     * @param tags Map representing tags to be added to files
     * @param language String representing language code to be used as container's prefix
     */
    public void migrate(String path,Map<String,String> tags,String language,List<String> outputTypes)
    {
        List<Path> files = getFiles(path);
        for (Path p : files)
        {
            String name = p.toString();
            if (Files.isRegularFile(p))
            {
                String file = name.substring(name.lastIndexOf('/')+1);
                String fileName = name.substring(0,name.lastIndexOf('/')+1);
                String truncate = fileName.substring(0, fileName.lastIndexOf('/'));
                String docType = truncate.substring(truncate.lastIndexOf('/')+1);
                String container = language + "-" + docType.toLowerCase().replaceAll(" ", "");
                uploadFolder(p.toFile().getPath(),container,tags,outputTypes);
                System.out.println(file + " uploaded to " + container);         
            }
            else 
            { 
                //file is not a folder
                System.out.println("Action failed.Please organise all files into folder.");
            }
        }
    }

    /** For migration of files from a particular folder in 'inbox' to respective containers based on
     * the subfolder that files are stored in
     * @folderName String representing name of folder whose documents are to be migrated
     * @tags Map representing the tags to be applied to all documents in 
     * @language String representing the language code to be used as the container's prefix
     * @ocrOutputType String representing the documents types that are produced from OCR (ie: "html"/"txt");
     */
    public void migrateFromInbox(Map<String,String> tags,String language,String ocrOutputType)
    {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient("inbox");
        List<BlobItem> blobList = containerClient.listBlobs().stream().collect(Collectors.toList());
        List<BlobItem> outputFiles = new ArrayList<>();

        //adding tags to every blob
        for (BlobItem blob: blobList)
        {
            String blobName = blob.getName();
            System.out.println("Adding tags for " +blobName);
            String extension = blobName.substring(blobName.lastIndexOf('.') + 1).toLowerCase();
            String fileName = blobName.substring(blobName.lastIndexOf('/')+1);
            String truncate = blobName.substring(0, blobName.lastIndexOf('/'));
            String docType = truncate.substring(truncate.lastIndexOf('/')+1);
            String containerFromDoc = docType.toLowerCase().replaceAll(" ", "");

            //process extension files after input files have been assigned tags
            if (extension.equals(ocrOutputType))
            {
                outputFiles.add(blob);
                continue;
            }    

            BlobClient blobClient = containerClient.getBlobClient(blobName);
            
            Map<String,String> newtags = new HashMap<>();
            newtags.putAll(tags);

            if (tags.get("DocType").equals("TBA"))
            {
                newtags.replace("DocType", docType);
            }
            if (tags.get("Project Name").equals("TBA"))
            {
                String projName = blobName.substring(0,blobName.indexOf("/"));
                newtags.replace("Project Name", projName);
            }
            if (blobName.contains("redacted"))
            {
                newtags.replace("Redacted","True");
            }

            blobClient.setTags(newtags);
            BlobContainerClient destContainerClient;
            String containerName = String.format("%s-%s",language,containerFromDoc);

            if (!blobServiceClient.getBlobContainerClient(containerName).exists())
            {
               destContainerClient = createContainer(containerName);
            }
            else{
                destContainerClient = blobServiceClient.getBlobContainerClient(containerName);
            }
            BlobClient destblobClient = destContainerClient.getBlobClient(String.format("%s/%s",extension,fileName));
            destblobClient.beginCopy(blobClient.getBlobUrl(), null);
            destblobClient.setTags(blobClient.getTags());   
        }

        //adding same tags for output file
        for (BlobItem blob : outputFiles)
        {
            String fileName = blob.getName().substring(0,blob.getName().lastIndexOf("."));
            String blobName = fileName.substring(fileName.lastIndexOf("/")+1);
            String extension = blob.getName().substring(blob.getName().lastIndexOf(".")+1);
            System.out.println(fileName);
            //original blob client to obtain tags from input file
            BlobClient blobClient = containerClient.getBlobClient(fileName);
            
            //blob client to add tags for output file
            String containerFromDoc = blobClient.getTags().get("DocType").toLowerCase().replaceAll(" ", "");
            String containerName = String.format("%s-%s",language,containerFromDoc);
            BlobContainerClient destContainerClient = blobServiceClient.getBlobContainerClient(containerName);
            BlobClient blobClient2 = destContainerClient.getBlobClient(String.format("%s/%s",extension,String.format("%s.%s",blobName,extension)));

            blobClient2.beginCopy(blobClient.getBlobUrl(), null);
            blobClient2.setTags(blobClient.getTags()); 
        }

    }

    /** Transferring blobs in container to test set to approximately 70 train: 30 test 
     * Excludes assignment of output documents to test set
     * @param container String representing container for function to be applied
     * @param outputType List of strings representing output types. Only input types will be 
     * transferred to test set & (if exists) the corresponding output tags will change accordingly.
     */
    public void transferToTest(String container,List<String> outputType)
    {
        int counter = 0; //counts number of items transferred to test
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
        PagedIterable<BlobItem> blobs = containerClient.listBlobs();
        int num = (int) blobs.stream().count();
        List<BlobItem> blobList = blobs.stream().collect(Collectors.toList());
        List<BlobClient> transferredBlobs = new ArrayList<>();

        if (num == 1)
        {
            System.out.println("No items transferred to test set");
            return;
        }

        //getting number of test documents to generate to maintain 70:30 ratio
        HashMap<String,Integer> map = listSetFields(container);
        int numTestFiles =(num/10)*3;
        int addTest = numTestFiles - map.get("Test");
        
        for (int i = 0; i <= addTest; i++)
        {
            int max = blobList.size();
            Random random = new Random();
            BlobItem updateBlob = blobList.get(random.nextInt(max-1));
            String extension = updateBlob.getName().substring(updateBlob.getName()
                    .lastIndexOf('.') + 1).toLowerCase();

            //if item chosen is output file, ignore
            while (outputType.contains(extension))
            {
                //get another blob randomly
                updateBlob = blobList.get(random.nextInt(max - 1));
                extension = updateBlob.getName().substring(updateBlob.getName()
                        .lastIndexOf('.') + 1).toLowerCase();
            }

            BlobClient blobClient = containerClient.getBlobClient(updateBlob.getName());
            transferredBlobs.add(blobClient);
            Map<String, String> dic = blobClient.getTags();
            dic.replace("Set","Test");
            blobClient.setTags(dic);
            counter++;

            //removing blob and updating max parameters
            blobList.remove(updateBlob);
            System.out.println("tag value changed to 'test' for " + blobClient.getBlobName());
        }
        for (BlobClient blobClient : transferredBlobs){
            String fullName = blobClient.getBlobName();
            String blobName = fullName.substring(fullName.indexOf("/")+1);

            //search for corresponding output files & update set values if it exists
            for (String fileType : outputType){
                BlobClient outputClient = containerClient.getBlobClient(String.format("%s/%s",fileType,String.format("%s.%s",blobName,fileType)));
                System.out.println(String.format("%s/%s",fileType,String.format("%s.%s",blobName,fileType)));
                if (outputClient.exists()){
                    outputClient.setTags(blobClient.getTags());
                }
            }
        }
        System.out.println(String.format("%d/%d input documents transferred to test set",counter,num));
    }

    /** Change value for specified tagged key for all blobs within container
     * @param container String representing container to be searched
     * @param field String representing key in map of tags to be changed
     * @param tagValue String representing value of key to be changed
     */
    public void changeTags(String container,String field, String tagValue)
    {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
        List<BlobItem> blobs = containerClient.listBlobs().stream().collect(Collectors.toList());

        for (BlobItem blob : blobs)
        {
            BlobClient blobClient = containerClient.getBlobClient(blob.getName());
            Map<String, String> dic = blobClient.getTags();
            dic.replace(field,tagValue);
            blobClient.setTags(dic);
            System.out.println("Setting tags");
        }
    }

    /** Change particular field in tags for items with specified sequence in file names
     * @param container String representing container containing files to have tags changed
     * @param field String representing key in map of tags to be changed
     * @param tagValue String representing updated tagged value for the specified key
     * @param sequence String identifying files whose tags are to be changed
     */
    public void changeTags(String container,String field, String tagValue,String sequence)
    {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
        List<BlobItem> blobs = containerClient.listBlobs().stream().collect(Collectors.toList());

        for (BlobItem blob : blobs)
        {
            if (blob.getName().contains(sequence))
            {
                BlobClient blobClient = containerClient.getBlobClient(blob.getName());
                Map<String, String> dic = blobClient.getTags();
                dic.replace(field,tagValue);
                blobClient.setTags(dic);
                System.out.println("Setting tags");
            }        
        }
    }

    /** Adds specified tags to specified file within container
     * @param container String representing container name
     * @param tags Map representing tags that are applied to documents
     */
    public void addTags(String container,Map<String,String> tags, String blobName)
    {
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.setTags(tags);
        System.out.println("Setting tags");
    }

    /** Creates a container with specified name
     * @param destContainerName String representing name of container to be created
     * @return BlobContainerClient to perform actions on containers
     */
    public BlobContainerClient createContainer(String destContainerName)
    {
        try
        {
            //Create the container in storage account. Fails if container already exist
            BlobContainerClient container = blobServiceClient.createBlobContainer(destContainerName);
            System.out.println("Created container " + container.getBlobContainerName());
            return container;
        }
        catch (BlobStorageException e)
        {
            System.out.println("Container already exists.");
        }
        return null;
    }

     /** Lists all document types found in all containers within storage account
     * Can be converted to table using Word, separating words at ","
     */
    public void listDocTypesAll()
    {
        List<BlobContainerItem> containers =blobServiceClient
                .listBlobContainers().stream().collect(Collectors.toList());

        for (BlobContainerItem containerItem : containers)
        {
            listDocTypes(containerItem.getName());
        }
    }

    /** Lists all document types found in specified container
     * Can be converted to table using Word, separating words at ","
     * @param container String representing container to be searched
     */
    public void listDocTypes(String container)
    {
        List<String> names = new ArrayList<>();
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
        List<BlobItem> blobs = containerClient.listBlobs().stream().collect(Collectors.toList());

        for (BlobItem b : blobs)
        {
            BlobClient blobClient = containerClient.getBlobClient(b.getName());
            Map<String, String> dic = blobClient.getTags();
            if (!names.contains(dic.get("DocType")))
            {
                names.add(dic.get("DocType"));
            }
        }
        names.forEach(x -> System.out.println(container + " , " + x));
    }

    /** Lists number of items assigned to train and test within container
     * @param container
     */
    public HashMap<String,Integer> listSetFields(String container)
    {
        HashMap<String,Integer> names = new HashMap<String,Integer>(){{
            put("Train",0);
            put("Test",0);
        }};

        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(container);
        List<BlobItem> blobs = containerClient.listBlobs().stream().collect(Collectors.toList());

        for (BlobItem b : blobs)
        {
            BlobClient blobClient = containerClient.getBlobClient(b.getName());
            Map<String, String> dic = blobClient.getTags();
            if ((dic.get("Set").equals("Train")))
            {
                names.replace("Train",names.get("Train")+1);
            }
            else if ((dic.get("Set").equals("Test")))
            {
                names.replace("Test",names.get("Test")+1);
            }
        }
        names.forEach((x,y) -> System.out.println( x +" : " +y));
        return names;
    }
}

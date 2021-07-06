package TaggingApplication;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

/** Class containing main method to run user commands from storageService
 * @author Charlene Pang
 */
public class TaggingApplication
{
    protected static HashMap<String,String> defaultTags;

    public static void main(String[] args) throws IOException
    {
        //Step 1: Initialisation of default tags
        Map<String,String> tags = setDefaultTags();

        /*Step 2: 
        After initialising default tags once, run this code block instead to continue making use 
        of default tags set
        "Defaults.txt" would be empty if default values have not been set. 
        File file = new File("Default Tags.txt");
        Scanner sc = new Scanner(file);
        String mapInputs = sc.nextLine();
        Map<String,String> tags = Arrays.stream(mapInputs.substring(1,mapInputs.length()-1).split(", "))
            .map(entry -> entry.split("="))
            .collect(Collectors.toMap(entry -> (String) entry[0], entry -> (String) entry[1]));

        sc.close();
        */

        //input connection string of storage account
        String connectStr = "";

        StorageService storage = new StorageService(connectStr);
        //default tag values. modify tag values if required

        //input the path to sourceFolder for uploading from Google Drive/from local device
        String sourceFolder = "";

        //input name of destination container 
        String container = "";

        //Language codes: cn-chinese, en-english, jp-japanese, ru-russian
        String language = "en";

        //uncomment functions to run.
        //storage.addTags(container, tags, blobName);
        //storage.changeTags(container, field, tagValue);
        //storage.changeTags(container, field, tagValue, sequence);
        //storage.createContainer(destContainerName)
        //storage.findDocType(value, containerName);
        //storage.listDocTypes(container);
        //storage.listDocTypesAll();
        //storage.listSetFields(container);
        //storage.migrate(path, tags, language);
        //storage.migrateFromInbox(tags, language, ocrOutputType);
        //storage.regroup(value, destContainerName, sourceContainerName);
        //storage.relocate(container1, container2);
        //storage.transferToTest(container, outputType);
        //storage.uploadFolder(path, container, tags, outputTypes);

        System.out.println("Done"); 
    }

    public static Map<String,String> setDefaultTags() throws FileNotFoundException
    {
        String docType,extractVers,OcrEngine,redacted,projName, set;
        Scanner sc = new Scanner(System.in);
        HashMap<String,String> tags;
        while (true)
        {
            tags = new HashMap<>();
            //getting default docType
            while(true)
            {
                System.out.println("Please enter default document type: (a/b)");
                docType = "TBA";
                System.out.println("a) To be assigned");
                System.out.println("b) Others");
                String input = sc.next().toLowerCase().replaceAll(" ","");

                while (!input.equals("a") && !input.equals("b"))
                {
                    System.out.println("Invalid input. Enter a/b.");
                    input = sc.next().toLowerCase().replaceAll(" ","");;
                }
                if (input.equals("b"))
                {
                    System.out.println("Input document type: ");
                    docType = sc.next();
                }
                System.out.println("Default document type selected is: " + docType + ".Confirm? (y/n)");
                String confirm = sc.next().toLowerCase().replaceAll(" ","");
                if (confirmation(confirm,sc,"docType"))
                {
                    //add default field to tag map after confirmation
                    tags.put("DocType",docType);
                    break;
                }
            }

            //getting default extract version
            while (true) 
            {
                System.out.println("Please enter default Extract Version:");
                extractVers = sc.next();
                System.out.println("Default Extract Version selected is: " + extractVers + ".Confirm? (y/n)");
                String confirm1 = sc.next().toLowerCase().replaceAll(" ","");
                if (confirmation(confirm1,sc,"Extract Version"))
                {
                    tags.put("Extract Version",extractVers);
                    break;
                }
            }               
            //getting default OCR Engine
            while (true)
            {
                System.out.println("Please enter default OCR Engine: (a/b/c)");
                OcrEngine = "ABBYY";
                System.out.println("a) ABBYY");
                System.out.println("b) Tessaract");
                System.out.println("c) Others");
                String input3 = sc.next().toLowerCase().replaceAll(" ","");
                while (!input3.equals("a") && !input3.equals("b") && !input3.equals("c"))
                {
                    System.out.println("Invalid input. Enter a/b/c.");
                    input3 = sc.next().toLowerCase().replaceAll(" ","");;
                }
                if (input3.equals("b"))
                {
                    OcrEngine = "Tesseract";
                }
                else if (input3.equals("c"))
                {
                    System.out.println("Input OCR Engine: ");
                    OcrEngine = sc.next();
                }
                System.out.println("Default OCR engine selected is: " + OcrEngine + ".Confirm? (y/n)");
                String confirm3 = sc.next().toLowerCase().replaceAll(" ","");
                if (confirmation(confirm3,sc,"OCR Engine"))
                {
                    tags.put("OCRengine",OcrEngine);
                    break;
                }
            }
           
            //getting default redacted value
            while (true)
            {
                System.out.println("Please enter default Redacted value: (a/b)");
                redacted = "True";
                System.out.println("a) True");
                System.out.println("b) False");
                String input4 = sc.next().toLowerCase().replaceAll(" ","");

                while (!input4.equals("a") && !input4.equals("b"))
                {
                    System.out.println("Invalid input. Enter a/b.");
                    input4 = sc.next().toLowerCase().replaceAll(" ","");
                }
                if (input4.equals("b"))
                {
                    redacted = "False";
                } 
                System.out.println("Default redacted value selected is: " + redacted + ".Confirm? (y/n)");
                String confirm4 = sc.next().toLowerCase().replaceAll(" ","");
                if (confirmation(confirm4,sc,"Redacted"))
                {
                    tags.put("Redacted",redacted);
                    break;
                }
            }

            //getting default projectName
            while (true)
            {
                System.out.println("Please enter default Project Name: (a/b)");
                projName = "TBA";
                System.out.println("a) To be assigned");
                System.out.println("b) Others");
                String input5 = sc.next().toLowerCase().replaceAll(" ","");  
                while (!input5.equals("a") && !input5.equals("b"))
                {
                    System.out.println("Invalid input. Enter a/b.");
                    input5 = sc.next().toLowerCase().replaceAll(" ","");
                }
                if (input5.equals("b"))
                {
                    System.out.println("Input Project Name: ");
                    projName = sc.next();
                }
                System.out.println("Default Project Name selected is: " + projName + ".Confirm? (y/n)");
                String confirm5 = sc.next().toLowerCase().replaceAll(" ","");
                if (confirmation(confirm5,sc,"Project Name"))
                {
                    tags.put("Project Name",projName);
                    break;
                } 
            }
        
            //getting default set value
            while (true)
            {
                System.out.println("Please enter default Set value: (a/b)");
                set = "Train";
                System.out.println("a) Train");
                System.out.println("b) Test");
                String input6 = sc.next().toLowerCase().replaceAll(" ","");

                while (!input6.equals("a") && !input6.equals("b"))
                {
                    System.out.println("Invalid input. Enter a/b.");
                    input6 = sc.next().toLowerCase().replaceAll(" ","");;
                }
                if (input6.equals("b"))
                {
                    set = "Test";
                } 
                System.out.println("Default Set value selected is: " + set + ".Confirm? (y/n)");
                String confirm6 = sc.next().toLowerCase().replaceAll(" ","");
                if (confirmation(confirm6,sc, "Set"))
                {
                    tags.put("Set",set);
                    break;
                }
            }
      
            System.out.println("Your selected default values are: ");
            System.out.println("DocType: " + docType);
            System.out.println("Extract Version: " + extractVers);
            System.out.println("Ocr Engine: " + OcrEngine);
            System.out.println("Redacted: " + redacted);
            System.out.println("Project Name: " +projName);
            System.out.println("Set: " + set); 
            System.out.println("Proceed? (y/n)");

            //final confirmation
            String confirm7 = sc.next().toLowerCase().replaceAll(" ","");

            if (confirmation(confirm7, sc ,"input"))
            {
                System.out.println("Default values set!");
                break;
            }
        }
        
        sc.close();
        File file = new File("Default Tags.txt"); 
        PrintWriter writer = new PrintWriter(file); 
        writer.write(tags.toString());
        writer.close();
        return tags;

}
/** Checks if input field is correct by user's input
     * @param input String representing user's previous input
     * @param sc Scanner object used to scan user's next input
     * @param field String to distinguish final confirmation from other confirmations
     * @return Returns true if input values are confirmed by user
     */
    public static boolean confirmation(String input,Scanner sc,String field){
        String confirm = input.toLowerCase().replaceAll(" ","");
        while (!confirm.equals("y") && !confirm.equals("n"))
        {
            System.out.println("Invalid input. Enter y/n.");
            confirm = sc.next().toLowerCase().replaceAll(" ","");
        }
        if (confirm.equals("n"))
        {
            if (field.equals("input"))
            {
                System.out.println("Restarting...Please re-enter all values!");
            }
            else
            {
                System.out.println("Restarting...Please enter values for " + field + " again!");
            }
            return false;
        }
        return true;
    }        
}

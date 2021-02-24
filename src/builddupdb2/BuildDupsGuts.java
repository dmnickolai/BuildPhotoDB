/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package builddupdb2;

import com.drew.imaging.ImageMetadataReader;
import com.drew.imaging.ImageProcessingException;
import com.drew.metadata.Directory;
import com.drew.metadata.Metadata;
import com.drew.metadata.exif.ExifSubIFDDirectory;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 *
 * @author Dennis
 */
public class BuildDupsGuts {
    
     // Global Constants
        // mySQL database constants
        static final String dbDriver = "com.mysql.cj.jdbc.Driver";
        static final String dbName = "photos_catalog";
        static final String dbUrl = "jdbc:mysql://localhost:3306/" + dbName;
        static final String dbUser ="dennis";
        static final String dbPassword = "55555";
        String baseTableName = "testBase2";
        String dupsTableName = baseTableName + "Dups2";
        static final String SQLcreateBaseColumns = 
                "(id INT NOT NULL AUTO_INCREMENT," +
                "timeStamp VARCHAR(40)," +
                "filePath VARCHAR(255) NOT NULL, fileSize INT, PRIMARY KEY(id)," +
                "UNIQUE KEY unique_timestamp(timeStamp), " +
                "INDEX ix_filesize (fileSize) ); ";
        static final String SQLcreateDupsColumns = 
                "(id INT NOT NULL AUTO_INCREMENT," +
                "timeStamp VARCHAR(40)," +
                "filePath VARCHAR(255) NOT NULL, " +
                "baseFilePath VARCHAR(255) NOT NULL, " +
                "fileSize INT, PRIMARY KEY(id)); ";
        String insertBaseSQL = "INSERT INTO " + baseTableName +
                 " (timestamp, filepath, filesize) "
                            + "VALUES (?, ?, ?);";
        PreparedStatement insertBase ;
        String insertDupsSQL = "INSERT INTO " + dupsTableName +
                 " (timestamp, filepath, basefilepath, filesize) "
                            + "VALUES (?, ?, ?, ?);";
        PreparedStatement insertDups;
        String selectByTimestampSQL = "SELECT * FROM " +
                baseTableName + " WHERE timestamp = ?;";
        
        PreparedStatement selectByTimestamp;
        //static final String dbName = "";
        // database connection, != null means connection open
        Connection dbConn = null;
        
        // statistic counters
        public int numFolders = 0;
        public int numTotal = 0;
        public int numDups = 0;
        
        // global variables
        static boolean processSubFolders = true;
        
        
    public BuildDupsGuts() {
        initialize();
        // get root folder
        File root = null;
        root = new File("P:\\Sorted Photos");
        // process root
        processFolder (root);

        //showResults();
        closeDbConnection();
    }
    
    void processFolder(File folder){
    // recursive routing to examine entries in folder
        numFolders += 1;
        int numOfFilesInFolder = 0;
        System.out.println("Processing folder: " + folder.getName());
        int numOfPhotosInFolder = 0;
        // If folder is empty, exit as nothing to process
        File[] fileList = folder.listFiles();
        if (fileList == null || fileList.length == 0) return;
        //int j = fileList.length;
        //process items in folder
        for (int i = 0; i< fileList.length; i++) {
            File thisItem = fileList[i];
            if (thisItem.isDirectory()) {
                if (processSubFolders) processFolder(thisItem);
            } else {
                numTotal += 1; 
                 
                if (isTargetFile(thisItem)) {
                    numOfFilesInFolder += 1;
                    processTargetItem(thisItem);
                }                
            }
        }  // end FOR iterating over folder items
        System.out.println( "\t" + numOfFilesInFolder + " in folder");
        if (numOfPhotosInFolder != 0) {
            logIt( numOfPhotosInFolder + " targets in " + folder.getPath() );
        }      
    }
    
    void processTargetItem (File thisFile) {
         String dateTaken = "";
        try {
            dateTaken = getExifDate(thisFile);
        
            if (dateTaken.isEmpty())  {                
                return;
            }
            String yearString = dateTaken.substring(0, 4);
            String monthString = dateTaken.substring(5,7);
            String dayOfMonthString = dateTaken.substring(8,10);
            //Validate timestamp
            int yearNumber = Integer.parseInt(yearString);               
            int monthNumber = Integer.parseInt(monthString);
            long fileSize = thisFile.length();
            String fullPathName = thisFile.getPath();
            // enter this file in database,
            //System.out.println("\t\t" + dateTaken + " " + fullPathName + "  " + fileSize);
            insertBase.setString(1,dateTaken);
            insertBase.setString(2, fullPathName);
            insertBase.setLong(3, fileSize);
            //System.out.println(insertBase.toString());
            insertBase.executeUpdate();
              // if duplicate, enter in duplicates
        } 
        catch (SQLException ex) {
            //System.out.println("SQL Ex =" + "(" + ex.getErrorCode() + ") " + ex.getMessage() + " on " + thisFile.getName() );
            if (ex.getErrorCode() == 1062) {
                //processDupFile(thisFile, dateTaken);
            } else{
                System.out.print("Unexpected SQL Error:" + ex.getMessage());}
                
        }
        catch (Exception e) { 
            System.out.println(e.getMessage() + " on " + thisFile.getName() );
        }
    
    }
    /*
    void processDupFile (File dupFile, int timestamp){
        numDups += 1;
        String basePath = getBasePath(timestamp);
        File baseFile = new File(basePath);
        if (!baseFile.exists()){
            System.out.println("Cannot open Base file " + basePath);
            System.exit(5);
        }
        if (!(dupFile.length() == baseFile.length())) {
            System.out.println("Lengths do not match");
            System.out.println("\t" + dupFile.getPath() + ":" + dupFile.length());
            System.out.println("\t" + baseFile.getPath() + ":" + baseFile.length());
        }
        if (!(dupFile.hashCode()== baseFile.hashCode())) {
            //System.out.println("Hashcodes do not match");
            //System.out.println("\t" + dupFile.getPath() + ":" + dupFile.hashCode());
            //System.out.println("\t" + baseFile.getPath() + ":" + baseFile.hashCode());
            // length don't match, log them
        }
        try {
            insertDups.setString(1, timestamp);
            insertDups.setString(2, dupFile.getPath());
            insertDups.setString(3, basePath);
            insertDups.setLong(4, dupFile.length());
            System.out.println(insertDups.toString());
            int i = insertDups.executeUpdate();
            if (i != 1) {
               System.exit(7);
            }
        
        }
        catch(SQLException ex){
            System.out.println("SQL Error on Insert Dups:" + ex.getMessage());
            System.exit(6);
        }
        // report files
        
    }

*/
    
    String getBasePath(String timestamp) {
        try {
        selectByTimestamp.setString(1, timestamp);
        //System.out.println(selectByTimestamp.toString());
        ResultSet rs = selectByTimestamp.executeQuery();
        String filepath = "";
        while(rs.next()) {
            filepath = rs.getString(1);
        }
        if (filepath.equals("")) {
            System.out.println("Base file not found for timestamp" + timestamp);
            System.exit(4);
        }
        rs.close();
        return filepath;
        }
        catch(SQLException ex){
            System.out.println("SQL Err in GetPath:" + ex.getMessage());
        }
        return null;
    }
    
    void initialize() {
        // get source folder
        // create table for photo timestamps, etc
        
        boolean testMode = true;
        dbConn = getDbConnection();
        try {
            // if testing, DROP tables and start clean
            if (testMode) {
                PreparedStatement tempSQL = dbConn.prepareStatement("DROP TABLE IF EXISTS " + baseTableName + ";");
                System.out.println(tempSQL.toString());
                tempSQL.executeUpdate();
                tempSQL = dbConn.prepareStatement("DROP TABLE IF EXISTS " + dupsTableName + ";");
                System.out.println(tempSQL.toString());
                tempSQL.executeUpdate();                
            }
           
            PreparedStatement createBaseTable = dbConn.prepareStatement(makeCreateSQL(baseTableName, SQLcreateBaseColumns));
            System.out.println(createBaseTable.toString());           
            createBaseTable.executeUpdate();
            
            PreparedStatement createDupsTable = dbConn.prepareStatement(makeCreateSQL(dupsTableName,SQLcreateDupsColumns ));
            System.out.println(createDupsTable.toString());           
            createDupsTable.executeUpdate();
            
            insertBase = dbConn.prepareStatement(insertBaseSQL);  
            selectByTimestamp = dbConn.prepareCall(selectByTimestampSQL);
            insertDups = dbConn.prepareCall(insertDupsSQL);
        }
        catch (SQLException ex) {
            System.out.println("Error in Initialize:" + ex.getMessage());
            System.exit(2);
        }       
    }
    
    // utility routines
    // return true if this is a file to process
    private boolean isTargetFile (File file) {
        // Validates file object that we want to process this item
        // Must be a file
        if (!file.isFile()) 
            return false;
        if (file.isHidden()) 
            return false;
        if (file.length()== 0) return false;
        if (validFileExtension(file)) return true;
        return false;
    }
    
    // examines file extension, return true if file to process
    protected boolean validFileExtension(File f) {   
        // File Extensions to process
        String[] targetExt = new String[] { "img", "jpg", "jpeg"};
        String name = f.getName();
        String ext = name.substring(name.lastIndexOf('.') + 1);
        for (int i = 0; i< targetExt.length; i++ ) {
            if (ext.equalsIgnoreCase(targetExt[i])) return true;
        }
        return false;
    } 
        
    // return date as string of date taken from EXIF     
    private String getExifDate(File file) {

        try {
          Metadata metadata = ImageMetadataReader.readMetadata(file);
          Directory directory = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
          int dateTag = ExifSubIFDDirectory.TAG_DATETIME_ORIGINAL;

          if (directory != null && directory.containsTag(dateTag)) {
            Date date = directory.getDate(dateTag, TimeZone.getDefault());
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(date);
          } else {
            return "";
          }
        } // end try
        catch (ImageProcessingException | IOException ex) {
              System.out.println(ex.getMessage());
              logIt ("Exif error: " + ex.getLocalizedMessage() + " for " + file.getPath());
              return "";
        }  //end catch
    }
    
    private String getDateFromInt (File file, int dateTag) {
        try {
          Metadata metadata = ImageMetadataReader.readMetadata(file);
          Directory directory = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
          //int dateTag = ExifSubIFDDirectory.TAG_DATETIME_ORIGINAL;

          if (directory != null && directory.containsTag(dateTag)) {
            Date date = directory.getDate(dateTag, TimeZone.getDefault());
            return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS").format(date);
          } else {
            return "";
          }
        } // end try
        catch (ImageProcessingException | IOException ex) {
              System.out.println(ex.getMessage());
              logIt ("Exif error: " + ex.getLocalizedMessage() + " for " + file.getPath());
              return "";
        }  //end catch
    }
    // return raw date from EXIF     
    private int getExifDate2(File file) {

        try {
          Metadata metadata = ImageMetadataReader.readMetadata(file);
          Directory directory = metadata.getFirstDirectoryOfType(ExifSubIFDDirectory.class);
          int dateTag = ExifSubIFDDirectory.TAG_DATETIME_ORIGINAL;

          if (dateTag != 0) {
            return dateTag;
          } else {
            return -1;
          }
        } // end try
        catch (ImageProcessingException | IOException ex) {
              System.out.println(ex.getMessage());
              logIt ("Exif error: " + ex.getLocalizedMessage() + " for " + file.getPath());
              return -1;
        }  //end catch
    }
    
    
    void logIt(String message){
        System.out.println(message);
    }
    
    //  MySQL database procedures
    
    Connection getDbConnection() {
        //if connection exists, return it
        if (dbConn != null) return dbConn;
        // open database connection
        try { 
            Class.forName(dbDriver);
            Connection conn = DriverManager.getConnection(dbUrl, dbUser,dbPassword);
            System.out.println("Connection OK");
            dbConn = conn;
            return conn;          
        }
        catch (SQLException ex){
            System.out.println("SQL Exception:" + ex.getMessage());
            System.out.println("Aborting app.....");
            System.exit(1);
        }
        catch (Exception ex){
            System.out.println(ex.getMessage());
            System.out.println("Aborting app.....");
            System.exit(1);
        }
        return null;
    }
        
    void closeDbConnection(){
        try{
            if (dbConn!= null)
                dbConn.close();
        }
        catch(SQLException ex) {
            // report error then ignore
            System.out.println("Exception in closs db connection, " + ex.getMessage());
        }
        finally {
            dbConn = null;
        }
    }
    
    String makeCreateSQL(String dbBaseName, String dbColumns) {
        return "CREATE TABLE IF NOT EXISTS " + dbBaseName + " " + dbColumns;
    }
// End of Class
}

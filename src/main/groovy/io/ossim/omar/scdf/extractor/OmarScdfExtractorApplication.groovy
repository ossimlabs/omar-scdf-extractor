package io.ossim.omar.scdf.extractor

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.zip.ZipFile
import org.apache.tika.Tika
import org.apache.tika.parser.gdal.GDALParser

@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfExtractorApplication {
  /**************************************************
  * The application logger
  ***************************************************/
  private final Logger logger = LoggerFactory.getLogger(this.getClass())

  /**************************************************
  * fileSource and fileDestination are variables
  * passed in from application.properties file.
  * They represent where to find the zip file you need
  * to extract (fileSource) and where to place the
  * extracted files (fileDestination).
  ***************************************************/
  @Value('${fileSource}')
  String fileSource

  @Value('${fileDestination}')
  String fileDestination

  /**************************************************
  * mediaTypeList contains the media types supported
  * by this class. The media types must be in the
  * format recognized by Apache Tika.
  * https://tika.apache.org/1.15/formats.html
  ***************************************************/
  final public static String[] MEDIA_TYPE_LIST = ['image/jpeg','image/tiff','image/nitf']

  /**
   * Constructor
   */
  OmarScdfExtractorApplication() {

    // Check for empty properties
    if(null == fileSource || fileSource.equals("")){
      fileSource = "/data/"
    }

    if(null == fileDestination || fileDestination.equals("")){
      fileDestination = "/data/"
    }
  }


  /***********************************************************
  *
  * Function: main
  * Purpose:  main method of the class.
  *
  * @param    args (String[])
  *
  ***********************************************************/
  static final void main(String[] args){
    SpringApplication.run OmarScdfExtractorApplication, args
  } // end method main


  /***********************************************************
  *
  * Function: receiveMsg
  * Purpose:  Takes a message, extract the file names, then send
  *           the zip file to the extractZipFileContent method.
  *           The extracted file name is then returned and
  *           message is sent.
  *
  * @param    message (Message<?>)
  *
  ***********************************************************/
  @StreamListener(Processor.INPUT)
  final void receiveMsg(final Message<?> message){
    if(logger.isDebugEnabled()){
      logger.debug("Message received: ${message}")
    }

    if(null != message.payload){
      if(logger.isDebugEnabled()) {
        logger.debug("Message payload: ${message.payload}")
      }

      final def parsedJson = new JsonSlurper().parseText(message.payload)
      if(parsedJson){
        parsedJson.files.each{file->
          if (file.contains("zip")){
            final String filePath = fileSource + file
            final File fileFromMsg = new File(filePath)

            /**************************************************
            * If statement that checks if fileFromMsg is empty.
            **************************************************/
            if (fileFromMsg.size() > 0){
              ZipFile zipFile = new ZipFile(fileFromMsg)
              final ArrayList<String> extractedFiles = extractZipFileContent(zipFile)
              extractedFiles.each{extractedFile->
                sendMsg(extractedFile)
              } // end extractedFiles.each
            } // end fileFromMsg.size() if statement
          } // end file.contains if statement
        } // end parsedJson.files.each
      } // end parseJason if statement
    } // end message.payload.length() if statement
} // end method receiveMsg


  /***********************************************************
  *
  * Function: sendMsg
  * Purpose:  Takes the path to a file, use it to build a Json
  *           message, then pass it on the Processor output pipe,
  *           which is defined in the application.properties file.
  *           (spring.cloud.stream.bindings.output.destination)
  *
  * @param    extractedFile (String)
  * @return   filesExtractedJson (converted to String)
  *
  ***********************************************************/
  @SendTo(Processor.OUTPUT)
  final String sendMsg(final String extractedFile){
    final JsonBuilder filesExtractedJson = new JsonBuilder()
    filesExtractedJson(files: extractedFile).toString()
  } // end method sendMsg


  /***********************************************************
  *
  * Function: extractZipFileContent
  * Purpose:  Takes a zip file from a specified location,
  *           unzip it and place the unzipped files in a
  *           specified location.  Both locations are
  *           defined by in the application.properties file.
  *           (fileSource, fileDestination)
  *
  * @param    zipFile (ZipFile)
  * @return   extractedFiles (ArrayList<String>)
  *
  ***********************************************************/
  ArrayList<String> extractZipFileContent(final ZipFile zipFile){
     /***********************************************
     * extractedFiles is used to store the full path
     * of the files extracted from the zip file.
     ***********************************************/
     final ArrayList<String> extractedFiles = new ArrayList<String>()

     /***********************************************
     * If statement that checks if zipfile is empty.
     ***********************************************/
     if (zipFile.size() > 0){
       /*****************************************************
       * zipFile.entries().each iterates through the zip file
       ******************************************************/
       zipFile.entries().each{
         /***********************************************
         * If statement that checks to make sure the
         * extracted is not a directory
         ***********************************************/
         if (!it.isDirectory()){
           final InputStream zinputStream = zipFile.getInputStream(it)
           final boolean isValidFile = checkType(zinputStream)

           /***********************************************
           * If statement that checks if the media type of
           * the extracted file is supported.
           ***********************************************/
           if (isValidFile){
             def fout = new File(fileDestination + File.separator + it.name)

             /***********************************************
             * Adds the fullpath to the extracted file to the
             * extractedFiles array list.
             ***********************************************/
             extractedFiles.add(fout.getAbsolutePath())

             InputStream fis = zipFile.getInputStream(it);
             FileOutputStream fos = new FileOutputStream(fout);
             byte[] readBuffer = new byte[1024];
             int length;
             while ((length = fis.read(readBuffer)) >= 0) {
               fos.write(readBuffer, 0, length);
             }
             fis.close();
             fos.close();
           }// end isValidFile if statement
         } // end it.isDirectory if statement
       } // end zipFile.entries().each
     } // end zipfile.side if statement

     zipFile.close()

     /***************************************************
     * deleteZipFile is used to delete the zip file after
     * all the files are extracted from it.
     ***************************************************/
     deleteZipFile(zipFile.getAbsolutePath())

     return extractedFiles
   } // end method extractZipFileContent


  /***********************************************************
  *
  * Function: checkType
  * Purpose:  Takes an InputStream check it's media type.
  *
  * @param    zinputStream (InputStream)
  * @return   isValid (boolean)
  *
  ***********************************************************/
  boolean checkType(final InputStream zinputStream){
    /*************************************************
    * The detect() method in the Tika class is used
    * to determine the media type of the InputSream.
    **************************************************/
    final Tika tika = new Tika()
    final String mediaType = tika.detect(zinputStream)

    /*************************************************
    * The variable being returned
    **************************************************/
    boolean isValid = false

    /*************************************************
    * Checks to see if the media type returned by the
    * Tika detect() method is in the mediaTypeList.
    **************************************************/
    if(OmarScdfExtractorApplication.MEDIA_TYPE_LIST.contains(mediaType)){
      isValid = true
    }
    return isValid
  } // end method checkType


  /***********************************************************
  *
  * Function: deleteZipFile
  * Purpose:  Deletes the file (zipFile) passed to the method
  *
  * @param    zipFile (String)
  *
  ***********************************************************/
  void deleteZipFile(final String zipFile){

    if(logger.isDebugEnabled()){
      logger.debug("Deleting file with name: ${zipFile}")
    }

    final File file = new File(zipFile)
    file.delete()
  } // end method deleteZipFile

} // end class OmarScdfExtractorApplication

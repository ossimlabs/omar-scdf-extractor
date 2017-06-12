package io.ossim.omar.scdf.extractor

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.core.io.support.ResourcePatternResolver
import org.springframework.messaging.Message
import org.springframework.messaging.handler.annotation.SendTo
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.core.io.ResourceLoader
import org.springframework.core.io.Resource
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import groovy.json.JsonOutput
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata
import com.amazonaws.services.s3.model.GetObjectRequest
import java.io.FileInputStream
import java.util.zip.ZipFile
import org.apache.tika.Tika
import org.apache.tika.parser.gdal.GDALParser
import java.util.zip.ZipInputStream
import java.io.FileInputStream

@SpringBootApplication
@EnableBinding(Processor.class)
class OmarScdfExtractorApplication {
  /**
  * The application logger
  */
  private final Logger logger = LoggerFactory.getLogger(this.getClass())

  /**
  * Variables passed in from application.properties
  */
  @Value('${fileSource}')
  String fileSource

  @Value('${fileDestination}')
  String fileDestination
  /***/

  /**
  * Class Variables
  */
  String[] mediaTypeList = ['image/jpeg','image/tiff','image/nitf','text/plain']
  /***/

  static final void main(String[] args){
    SpringApplication.run OmarScdfExtractorApplication, args
  }

  @StreamListener(Processor.INPUT)
  final void receiveMsg(final Message<?> message){

    if(logger.isDebugEnabled()){
      logger.debug("Message received: ${message}")
    }

    logger.debug("Message payload: ${message.payload}")

    if(message.payload != null){
      final def parsedJson = new JsonSlurper().parseText(message.payload)
      logger.debug("parsedJson outside if statement: ${parsedJson}")
      if(parsedJson){
        logger.debug("parsedJson inside if statement: ${parsedJson}")
        parsedJson.files.each{file->
            if (file.contains("zip")){
              final String[] extractedFiles = extractZipFileContent(file)
              extractedFiles.each{extractedFile->
                sendMsg(extractedFile)
              }
            }
        }
      }
    }
}

  @SendTo(Processor.OUTPUT)
  final String sendMsg(final String extractedFile){
    final JsonBuilder filesExtractedJson = new JsonBuilder()
    filesExtractedJson(files: extractedFile).toString()
  }

  String[] extractZipFileContent(String zipFileName){
     String zipFilePath = fileSource + zipFileName
     ZipFile zipFile = new ZipFile(new File(zipFilePath))
     String[] extractedFiles = []
     zipFile.entries().each{
       if (!it.isDirectory()){
         InputStream zinputStream = zipFile.getInputStream(it)
         boolean isValidFile = checkType(zinputStream)
         if (isValidFile){
           def fOut = new File(fileDestination + File.separator + it.name)
           extractedFiles.add(fOut.getAbsolutePath())
           new File(fOut.parent).mkdirs()
           def fos = new FileOutputStream(fOut)
           def buf = new byte[it.size]
           def len = zipFile.getInputStream(it).read(buf)
           fos.write(buf, 0, len)
           fos.close()
         }
       }
     }
     zipFile.close()
     deleteZipFile(zipFilePath)

     return extractedFiles
   }

  boolean checkType(InputStream zinputStream){
    Tika tika = new Tika()
    boolean isValid = false
    String mediaType = tika.detect(zinputStream)

    if(mediaTypeList.contains(mediaType)){
      isValid = true
    }
    return isValid
  }

  void deleteZipFile(String zipFile){
    File file = new File(zipFile)
    file.delete()
  }
}

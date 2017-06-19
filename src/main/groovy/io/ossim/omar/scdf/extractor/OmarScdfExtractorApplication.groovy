package io.ossim.omar.scdf.extractor

import groovy.util.logging.Slf4j
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.cloud.stream.annotation.EnableBinding
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.cloud.stream.annotation.StreamListener
import org.springframework.cloud.stream.messaging.Processor
import org.springframework.messaging.Message
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.support.MessageBuilder
import org.springframework.cloud.stream.annotation.Output
import org.springframework.cloud.stream.messaging.Source
import groovy.json.JsonSlurper
import groovy.json.JsonBuilder
import java.util.zip.ZipFile
import org.apache.tika.Tika

@SpringBootApplication
@EnableBinding(Processor.class)
@Slf4j
class OmarScdfExtractorApplication
{
    /**************************************************
    * Output channel to send messages on
    ***************************************************/  @Autowired
    @Output(Source.OUTPUT)
    private MessageChannel outputChannel

    /**************************************************
    * mediaTypeList contains the media types supported
    * by this class. The media types must be in the
    * format recognized by Apache Tika.
    * https://tika.apache.org/1.15/formats.html
    ***************************************************/
    final public static String[] MEDIA_TYPE_LIST = ['image/jpeg','image/tiff','image/nitf']

    /***********************************************************
    *
    * Function: main
    * Purpose:  main method of the class.
    *
    * @param    args (String[])
    *
    ***********************************************************/
    static final void main(String[] args)
    {
        SpringApplication.run OmarScdfExtractorApplication, args
    } // end method main


    /***********************************************************
    *
    * Function: receiveMsg
    * Purpose:  Receives a message and extracts the zip file
    *           name from the message payload.  It will then
    *           pass both the file name and the message to the
    *           extractZipFileContent method.
    *
    * @param    message (Message<?>)
    *
    ***********************************************************/
    @StreamListener(Processor.INPUT)
    final void receiveMsg(final Message<?> message)
    {
        log.debug("Message received: ${message}")

        if(null != message.payload)
        {
            log.debug("Message payload: ${message.payload}")

            final def parsedJson = new JsonSlurper().parseText(message.payload)
            if (parsedJson)
            {
                parsedJson.files.each { file ->
                    if (file.endsWith("zip"))
                    {
                        final File fileFromMsg = new File(file)

                        /**************************************************
                        * If statement that checks if fileFromMsg is empty.
                        **************************************************/
                        if (fileFromMsg.size() > 0)
                        {
                            ZipFile zipFile = new ZipFile(fileFromMsg)
                            if (zipFile.size() > 0)
                            {
                                extractZipFileContent(zipFile, message)
                            }
                        } // end fileFromMsg.size() if statement
                    } // end file.contains if statement
                    else if (!new File(file).isDirectory())
                    {
                        // send message along the chain if it isn't of type .zip
                        sendMsg(file, message)
                    }
                } // end parsedJson.files.each
            } // end parseJason if statement
        } // end message.payload if statement
    } // end method receiveMsg


    /***********************************************************
    *
    * Function: sendMsg
    * Purpose:  Receives the path to a file which will be used
    *           as a payload to the message that will be created
    *           and a message whose header will be copied to the
    *           newly created message.  The message is then
    *           sent on Processor output stream which is defined
    *           in the application.properties file.
    *           (spring.cloud.stream.bindings.output.destination)
    *
    * @param    extractedFile (String)
    * @param    message (Message<?>)
    *
    ************************************************************/
    final void sendMsg(final String extractedFile, final Message<?> message)
    {
        final JsonBuilder filesExtractedJson = new JsonBuilder()
        filesExtractedJson(filename: extractedFile)
        log.debug("Message Sent: ${filesExtractedJson.toString()}")
        Message<String> msgToSend = MessageBuilder.withPayload(filesExtractedJson.toString())
            .copyHeaders(message.getHeaders())
            .build()

            outputChannel.send(msgToSend)
    } // end method sendMsg


    /***********************************************************
    *
    * Function: extractZipFileContent
    * Purpose:  Takes a zip file from a specified location,
    *           unzip it and place the unzipped files in a
    *           specified location.  Both locations are
    *           passed in by the Extractor
    *
    * @param    zipFile (ZipFile)
    * @param    message (Message<?>)
    *
    ***********************************************************/
    void extractZipFileContent(final ZipFile zipFile, final Message<?> message)
    {
        /***********************************************
        * extractedFiles is used to store the full path
        * of the files extracted from the zip file.
        ***********************************************/
        final String extractedFile

        /***********************************************
        * If statement that checks if zipfile is empty.
        ***********************************************/
        if (zipFile.size() > 0)
        {
            /*****************************************************
            * zipFile.entries().each iterates through the zip file
            ******************************************************/
            zipFile.entries().each
            { zipEntry ->
                /***********************************************
                * If statement that checks to make sure the
                * extracted is not a directory
                ***********************************************/
                if (!zipEntry.isDirectory())
                {
                    final InputStream zinputStream = zipFile.getInputStream(zipEntry)
                    zipFile.getName()
                    final boolean isValidFile = checkType(zinputStream)

                    /***********************************************
                    * If statement that checks if the media type of
                    * the extracted file is supported.
                    ***********************************************/
                    if (isValidFile)
                    {
                        File zipParent = new File(new File(zipFile.getName()).parent)
                        File fout = new File(zipParent.getAbsolutePath() + "/" + zipEntry.name)

                        /***********************************************
                        * Makes the parent directory of the file that
                        * was extracted from the zip file.
                        ************************************************/
                        new File(fout.parent).mkdirs()

                        /***********************************************
                        * Adds the fullpath to the extracted file to the
                        * extractedFiles array list.
                        ***********************************************/
                        extractedFile = fout.getAbsolutePath()

                        InputStream fis = zipFile.getInputStream(zipEntry)
                        FileOutputStream fos = new FileOutputStream(fout)
                        byte[] readBuffer = new byte[1024]
                        int length
                        while ((length = fis.read(readBuffer)) >= 0)
                        {
                            fos.write(readBuffer, 0, length)
                        }

                        fis.close()
                        fos.close()

                        sendMsg(extractedFile, message)
                    }// end isValidFile if statement
                } // end zipEntry.isDirectory if statement
            } // end zipFile.entries().each
            zipFile.close()
        } // end zipfile.side if statement

        /***************************************************
        * deleteZipFile is used to delete the zip file after
        * all the files are extracted from it.
        ***************************************************/
        deleteZipFile(zipFile.getName())
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
    boolean checkType(final InputStream zinputStream)
    {
        /*************************************************
        * The detect() method in the Tika class is used
        * to determine the media type of the InputSream.
        **************************************************/
        final String mediaType = new Tika().detect(zinputStream)

        /*************************************************
        * Checks to see if the media type returned by the
        * Tika detect() method is in the mediaTypeList.
        **************************************************/
        return OmarScdfExtractorApplication.MEDIA_TYPE_LIST.contains(mediaType)
    } // end method checkType


    /***********************************************************
    *
    * Function: deleteZipFile
    * Purpose:  Takes the path to the zip file (zipFile), then
    *           deletes it.
    *
    * @param    zipFile (String)
    *
    ***********************************************************/
    void deleteZipFile(final String zipFile)
    {
        log.debug("Deleting file with name: ${zipFile}")

        final File file = new File(zipFile)
        file.delete()
    } // end method deleteZipFile
} // end class OmarScdfExtractorApplication

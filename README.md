# omar-scdf-extractor
The Extractor is a Spring Cloud Data Flow (SCDF) Processor.
This means it:
1. Receives a message on a Spring Cloud input stream using Kafka.
2. Performs an operation on the data.
3. Sends the result on a Spring Cloud output stream using Kafka to a listening SCDF Processor or SCDF Sink.

## Purpose
The Exctractor receives a JSON message from the Downloader containing a list of files downloaded, including a zip file. The Extractor then extracts the zip file, which should contain images (.tif, .ntf, .jpeg, etc). When an image is extracted, the extractor sends a message to the Stager containing the filename and full path to the image. In some instances, the S3 Uploader will also be listening to the Extractor's output stream in order to upload the extracted images to an S3 Bucket.

## JSON Input Example (from the Downloader)
```json
{
   "files":[
      "/data/2017/06/22/09/933657b1-6752-42dc-98d8-73ef95a5e780/12345/SCDFTestImages.zip",
      "/data/2017/06/22/09/933657b1-6752-42dc-98d8-73ef95a5e780/12345/SCDFTestImages.zip_56734.email"
   ]
}
```

## JSON Output Example (to the Stager and/or S3 Uploader)
```json
{
   "filename":"/data/2017/06/22/09/933657b1-6752-42dc-98d8-73ef95a5e780/12345/SCDFTestImages/tiff/14SEP12113301-M1BS-053951940020_01_P001.TIF"
}
```

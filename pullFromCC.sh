#!/bin/bash
#
# iterates on the segments to be processed

if [ "$AWS_ACCESS_KEY" = "" ]; then
    echo "AWS_ACCESS_KEY not set"
    # exit -1;
fi

if [ "$AWS_SECRET_KEY" = "" ]; then
    echo "$AWS_SECRET_KEY not set"
    # exit -1;
fi

queue=`aws --region us-east-1 sqs get-queue-url --queue-name tika --output text`

# TODO Hadoop common config : number reducers etc...

while true; do 

  if [ -e ".STOP" ]
  then
   echo "STOP file found - escaping loop"
   break
  fi

# poll the queue for a segment
aws --region us-east-1 sqs receive-message --queue-url $queue --output text > message

segment=`cat message | cut -f2`
handle=`cat message | cut -f5`

rm message

if [ "$segment" = "" ] 
then
   echo "No segment found in queue $queue"
   break
fi

echo "Processing $segment"

segName=`echo $segment | sed 's/segments\//	/' | cut -f2`

# hadoop jar io/target/behemoth-io-*-SNAPSHOT-job.jar com.digitalpebble.behemoth.io.warc.WARCConverterJob -D fs.s3n.awsAccessKeyId=$AWS_ACCESS_KEY -D fs.s3n.awsSecretAccessKey=$AWS_SECRET_KEY -D document.filter.mimetype.keep=.+[^html] $segment/warc/*.warc.gz $segName

hadoop jar io/target/behemoth-io-*-SNAPSHOT-job.jar com.digitalpebble.behemoth.io.warc.WARCConverterJob -D document.filter.mimetype.keep=.+[^html] $segment/warc/CC-*.warc.gz $segName

RETCODE=$?
  
  if [ $RETCODE -ne 0 ] 
  then 
   # try again later
   aws --region us-east-1 sqs change-message-visibility --queue-url $queue --receipt-handle $handle --visibility-timeout 3600
   continue
  fi

# export to the local FS
./behemoth exporter -i $segName -n URL -o file:///mnt/$segName -b

RETCODE=$?
  
 if [ $RETCODE -ne 0 ] 
  then 
   # try again later
   aws --region us-east-1 sqs change-message-visibility --queue-url $queue --receipt-handle $handle --visibility-timeout 3600
   continue
  fi

hadoop fs -rmr $segName

# push to server
scp -i ~/Ju.pem /mnt/$segName jnioche@162.209.99.130:$segName

# remove from queue
aws --region us-east-1 sqs delete-message --queue-url  $queue --receipt-handle $handle

rm -rf /mnt/$segName

done

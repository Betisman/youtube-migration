const fs = require('fs');
// const ytdl = require('ytdl-core');
const AWS = require('aws-sdk');
const stream = require('stream');
const EventEmitter = require('events');
const axios = require('axios');
const R = require('ramda');
const { S3, GetObjectCommand, HeadObjectCommand, PutObjectCommand } = require('@aws-sdk/client-s3');
const webm2mp4 = require('./webm2mp4');

const stream2String = stream =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });

try {
  const s3 = new S3({ region: 'eu-west-1' });

  const eventEmitter = new EventEmitter();

  // const bucket = 'streamvideotestbetisman';
  const bucket = 'gs-youtube-migration';
  const mp4Bucket = 'gs-youtube-migration/mp4';

  const infoProgress = {
    currentVideo: '',
    downloadVideo: {
      downloaded: 0,
      percent: 0,
    },
    downloadTransform: {
      downloaded: 0,
      percent: 0,
    },
    upload: {
      uploaded: 0,
      percent: 0,
    },
  };
  const progressHandler = {
    set(obj, prop, value) {
      obj[prop] = value;
      eventEmitter.emit('progressChanged');
    }
  }
  let proxyProgress = new Proxy(infoProgress, progressHandler);

  const prettyBytes = (bytes, decimals = 2) => {
    if (bytes === 0) return '0 Bytes';

    const k = 1024;
    const dm = decimals < 0 ? 0 : decimals;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];

    const i = Math.floor(Math.log(bytes) / Math.log(k));

    return (bytes / Math.pow(k, i)).toFixed(dm) + ' ' + sizes[i];
  }

  const printMessage = message => {
    process.stdout.clearLine();
    process.stdout.cursorTo(0);
    process.stdout.write(message);
  }

  eventEmitter.removeListener('progressChanged', args => console.log('remove listener progressChanged', args));
  eventEmitter.on('progressChanged', () => {
    printMessage(`${proxyProgress.currentVideo} ||| audio(${(proxyProgress.downloadAudio.percent).toFixed(2)}% / ${prettyBytes(proxyProgress.downloadAudio.downloaded)}) video(${(proxyProgress.downloadVideo.percent).toFixed(2)}% / ${prettyBytes(proxyProgress.downloadVideo.downloaded)}) merge(${(proxyProgress.downloadMerge.percent).toFixed(2)}% / ${prettyBytes(proxyProgress.downloadMerge.downloaded)}) ||| uploaded ${(proxyProgress.upload.percent).toFixed(2)}% / ${prettyBytes(proxyProgress.upload.uploaded)}`);
  });

  (async () => {

    console.log("Beginning json2s3");

    let uploadedVideosS3;
    try {
      console.log('Downloading S3 manifest');
      const command = new GetObjectCommand({ Bucket: bucket, Key: '_s3videos_uploaded.json' });
      const { Body } = await s3.send(command);
      const bodyContent = await stream2String(Body);
      uploadedVideosS3 = JSON.parse(bodyContent);
      console.log('Downloaded S3 manifest');
      // console.log(JSON.stringify({ uploadedVideosS3 }, null, 2))
    } catch (error) {
      console.error(error)
      if (error.code === 'ENOENT') {
        uploadedVideosS3 = {};
      }
      else throw error;
    }

    console.log(uploadedVideosS3.length);
    console.log('uploadedVideosS3', Object.keys(uploadedVideosS3));
    let index = 0;
    for await (let [videoId, video] of Object.entries(uploadedVideosS3).slice(0, 1)) {
      try {
        // console.log(video)
        console.log('\n' + index + '\n');

        const {
          uploaded: {
            Key: videoKey,
          },
          uploadedInfo: {
            Key: jsonKey,
          }
        } = video;

        console.log(`Getting ${videoKey} stream from S3`);

        const command = new GetObjectCommand({
          Bucket: bucket,
          Key: videoKey,
        });
        const { Body } = await s3.send(command);

        const videoStream = Body;
        let progress = 0;
        videoStream.on('data', chunk => {
          progress += chunk.length;
          proxyProgress.download = {
            downloaded: progress,
            percent: progress / metadata.ContentLength * 100,
          }
        });

        const transformStream = webm2mp4({ videoStream, options: {}, eventEmitter });

        youtubeStream.on('error', error => {
          console.log('Error on youtubeStream');
          console.error(error);
        });

        let downloadedVideoSize = 0;
        let downloadedMergeSize = 0;
        ['onVideoProgress', 'onResultProgress', 'onVideoError', 'onResultError'].forEach(event =>
          eventEmitter.rawListeners(event).forEach(listener => eventEmitter.removeListener(event, listener)));
        eventEmitter.removeListener('onVideoProgress', args => console.log('remove listener onVideoProgress', args));
        eventEmitter.removeListener('onResultProgress', args => console.log('remove listener onResultProgress', args));
        eventEmitter.removeListener('onVideoError', args => console.log('remove listener onVideoError', args));
        eventEmitter.removeListener('onResultError', args => console.log('remove listener onResultError', args));
        eventEmitter.on('onVideoProgress', (chunkLength, downloaded, total) => {
          downloadedVideoSize += chunkLength;
          proxyProgress.downloadVideo = {
            downloaded: downloadedVideoSize,
            percent: (downloaded / total) * 100,
          }
        });
        eventEmitter.on('onResultProgress', (chunkLength) => {
          downloadedMergeSize += chunkLength;
          proxyProgress.downloadMerge = {
            downloaded: downloadedMergeSize,
            percent: NaN,
          }
        });
        eventEmitter.on('onVideoError', error => {
          console.error('ve', error);
        });
        eventEmitter.on('onResultError', error => {
          console.error('me', error);
        });


        const uploadCommand = new PutObjectCommand({
          Bucket: mp4Bucket,
          Key: `${videoKey}.mp4`,
        });
        const { Body: body } = await s3.send(uploadCommand);

        body.on('finish', () => console.log('finish'));
        body.on('end', () => console.log('end'));
        body.on('error', (e) => console.error('error', e));

        const upload2S3 = async videoStream => {
          // if (isUploadedS3(key)) {
          //   console.log(`Skipping already uploaded ${key}`);
          //   return;
          // }

          let uploadedVideoInfo;
          console.log('to Upload')
          uploadedVideoInfo = await upload(S3)(bucket, key)(videoStream);
          console.log('uploaded??')

          console.log('\n');

          const infoStream = new stream.Readable.from([JSON.stringify({
            ...data,
            uploaded: uploadedVideoInfo || {},
            data,
            metadata: {
              uploaded: Date.now(),
            },
            videoInfo,
          })]);
          const uploadedJsonInfo = await upload(S3)(bucket, `${key}.json`)(infoStream);

          console.log(`\n${data.id} successfully uploaded`);

          uploadedVideosS3 = {
            ...uploadedVideosS3,
            [data.id]: {
              ...uploadedVideosS3[data.id],
              uploaded: uploadedVideoInfo || {},
              uploadedInfo: uploadedJsonInfo,
              data,
              metadata: {
                uploaded: Date.now(),
                size: proxyProgress.upload.uploaded,
              },
            },
          };


          const manifestStream = new stream.Readable.from([JSON.stringify(uploadedVideosS3 || {}, null, 2)]);
          await upload(S3)(bucket, '_s3videos_uploaded.json')(manifestStream);
          await download(S3)(bucket)('_s3videos_uploaded.json');

          return uploadedVideosS3;
        };

        const uploadedVideo2S3 = await upload2S3(transformStream);
        console.log('pepepepitop')

        index += 1;
      } catch (error) {
        if (error.statusCode === 503) {
          console.log(`WARNING!!: ${key} had to be skipped due to a 503 error ******************************************************************************`);
          continue;
        }
        console.log('-------------------------------------------------')
        console.error(error?.response?.data?.error || error);
        console.log('=================================================')
        index += 1;
        continue;
      } finally {
        eventEmitter.removeListener('onAudioError', args => console.log('remove listener', args));
        eventEmitter.removeListener('onVideoError', args => console.log('remove listener', args));
        eventEmitter.removeListener('onResultError', args => console.log('remove listener', args));
      }
    };
  })();

} catch (error) {
  console.log('Critical error: ******************************************************************************************');
  console.error(error);
}
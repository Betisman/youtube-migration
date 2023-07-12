const fs = require('fs');
// const ytdl = require('ytdl-core');
const AWS = require('aws-sdk');
const stream = require('stream');
const EventEmitter = require('events');
const axios = require('axios');
const oneDriveAPI = require("onedrive-api");
const ytmux = require('./ytmux');
const { youtube } = require('googleapis/build/src/apis/youtube');
const R = require('ramda');
const ytdl = require('ytdl-core');

try {
  const S3 = new AWS.S3();

  const eventEmitter = new EventEmitter();

  const bucket = 'gs-youtube-migration';

  const infoProgress = {
    currentVideo: '',
    downloadAudio: {
      downloaded: 0,
      percent: 0,
    },
    downloadVideo: {
      downloaded: 0,
      percent: 0,
    },
    downloadMerge: {
      downloaded: 0,
      percent: 0,
    },
    upload: {
      uploaded: 0,
      percent: 0,
    },
    upload2Sharepoint: {
      uploaded: 0,
      percent: 0,
    }
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

  const upload = S3client => (BUCKET, KEY) => videoStream => new Promise((resolve, reject) => {
    let pass = new stream.PassThrough();

    videoStream.pipe(pass)
    videoStream.on('error', error => {
      console.log('inside upload=======================================================================================================================================');
      pass.emit('error', error)
      pass.destroy();
      // return reject(error)
    });

    let params = {
      Bucket: BUCKET,
      Key: KEY,
      Body: pass,
    };

    S3client.upload(params, function (error, data) {
      if (error) {
        console.log(`Error uploading to S3===================================================================================================================`);
        console.error(error);
        reject(error);
      } else {
        resolve(data);
      }
    }).on('httpUploadProgress', progress => {
      proxyProgress.upload = {
        uploaded: progress.loaded,
        percent: (progress.loaded / progress.total) * 100,
      }
    });

    return pass;
  });

  const download = S3Client => BUCKET => KEY => {
    let params = {
      Bucket: BUCKET,
      Key: KEY,
    };

    return S3Client.getObject(params, (err, data) => {
      if (err) {
        console.log(`Error downloading from S3 ${BUCKET} ${KEY}`)
        console.error(err);
        throw error;
      }
      fs.writeFileSync('_s3videos_uploaded.json', data?.Body ?? JSON.stringify({}), { encoding: 'utf-8' });
    });
  }

  (async () => {

    console.log("Beginning json2s3");

    const videos = JSON.parse(fs.readFileSync('all_the_videos.json', { encoding: 'utf-8' }));
    let uploadedVideosS3;
    try {
      await download(S3)(bucket)('_s3videos_uploaded.json');
      uploadedVideosS3 = JSON.parse(fs.readFileSync('_s3videos_uploaded.json', { encoding: 'utf-8' }));
      // uploadedVideosS3 = {};
    } catch (error) {
      if (error.code === 'ENOENT') {
        uploadedVideosS3 = {};
      }
      else throw error;
    }

    console.log(videos.length);
    console.log('uploadedVideosS3', Object.keys(uploadedVideosS3));
    // console.log('uploadedVideosOneDrive', Object.keys(uploadedVideosOneDrive));


    // CHECK VIDEOS
    console.log('videos', videos.length);
    console.log('uploadedVideosS3', Object.keys(uploadedVideosS3).length);

    const checkLists = (listA, listB) => R.difference(listA, listB);

    const pluckVideos = R.pluck('videoId')(R.pluck('contentDetails')(videos));
    // console.log('pluckVideos', pluckVideos)
    const diff = checkLists(pluckVideos, Object.keys(uploadedVideosS3))
    console.log('Videos missing in S3:', diff)
    // console.log(videos.find(video => video.contentDetails.videoId === '2NeMGV3aCKk'))
    // diff.forEach(v => console.log(`https://www.youtube.com/watch?v=${v} - ${videos.find(video => video.contentDetails.videoId === v).snippet.title}`));






    const isUploadedS3 = videoId => Object.entries(uploadedVideosS3).filter(([key, value]) => key === videoId && value?.uploadedInfo?.key).length;

    let index = 0;
    const cabritosVideos = [
        // '_E9gkTjE_EQ',
        // 'K2RMOuJsFns',
        // 'HBBpQora-g8',
        // 'wOHWVHYkiTI',
      ]

    // for await (let video of videos) {
      // for await (let video of [videos.find(video => video.contentDetails.videoId === '_E9gkTjE_EQ')]) {
    for await (let video of [videos.find(video => cabritosVideos.indexOf(video.contentDetails.videoId) > -1)]) {
      console.log(`\n${index}/${videos.length}\n`);
      // if (index > 1) return;

      const {
        contentDetails: {
          videoId,
          videoPublishedAt,
        },
        status: {
          privacyStatus,
        },
        snippet: {
          title,
          description,
        }
      } = video

      console.log(!cabritosVideos.find(v => v === videoId))
      if (!cabritosVideos.find(v => v === videoId)) {
        console.log(`no video cabrito ${videoId}`);
        index += 1;
        continue;
      }
;
      const data = {
        id: videoId,
        publishedAt: videoPublishedAt,
        privacyStatus,
        title,
        description,
        url: `https://www.youtube.com/watch?v=${videoId}`,
      };

      const key = data.id;
      try {
        proxyProgress.currentVideo = data.id;

        // if (isUploadedS3(data.id)) {
        //   console.log(`Skipping already uploaded ${data.id}`);
        //   index += 1;
        //   continue;
        // }

        let options;
        let youtubeStream;
        console.log('cabritosVideos', cabritosVideos.find(v => v === data.id))
        if (cabritosVideos.find(v => v === data.id)) { 
          options = { video: { quality: 'highest' } };
          // youtubeStream = ytdl(data.url, { quality: 'highest' });
          if (data.id = 'HBBpQora-g8') {
            youtubeStream = ytdl(data.url, {});
          } else {
            youtubeStream = ytmux({ link: data.url, eventEmitter, options });
          }
        } else {
          youtubeStream = ytmux({ link: data.url, eventEmitter, options });
        }
        console.log('options', options)

        const videoInfo = await ytmux.getInfo(data.id);
        // console.log(JSON.stringify({info}, null, 2))
        console.log(JSON.stringify({ info: videoInfo.formats}))

        // const youtubeStream = ytmux({ link: data.url, eventEmitter, options });
        youtubeStream.on('error', error => {
          console.log('Error on youtubeStream');
          console.error(error);
        });

        let downloadedAudioSize = 0;
        let downloadedVideoSize = 0;
        let downloadedMergeSize = 0;
        ['onAudioProgress', 'onVideoProgress', 'onMergeProgress', 'onAudioError', 'onVideoError', 'onMergeError'].forEach(event =>
          eventEmitter.rawListeners(event).forEach(listener => eventEmitter.removeListener(event, listener)));
        eventEmitter.removeListener('onAudioProgress', args => console.log('remove listener onAudioProgress', args));
        eventEmitter.removeListener('onVideoProgress', args => console.log('remove listener onVideoProgress', args));
        eventEmitter.removeListener('onMergeProgress', args => console.log('remove listener onMergeProgress', args));
        eventEmitter.removeListener('onAudioError', args => console.log('remove listener onAudioError', args));
        eventEmitter.removeListener('onVideoError', args => console.log('remove listener onVideoError', args));
        eventEmitter.removeListener('onMergeError', args => console.log('remove listener onMergeError', args));
        eventEmitter.on('onAudioProgress', (chunkLength, downloaded, total) => {
          downloadedAudioSize += chunkLength;
          proxyProgress.downloadAudio = {
            downloaded: downloadedAudioSize,
            percent: (downloaded / total) * 100,
          }
        });
        eventEmitter.on('onVideoProgress', (chunkLength, downloaded, total) => {
          downloadedVideoSize += chunkLength;
          proxyProgress.downloadVideo = {
            downloaded: downloadedVideoSize,
            percent: (downloaded / total) * 100,
          }
        });
        eventEmitter.on('onMergeProgress', (chunkLength) => {
          downloadedMergeSize += chunkLength;
          proxyProgress.downloadMerge = {
            downloaded: downloadedMergeSize,
            percent: NaN,
          }
        });
        eventEmitter.on('onAudioError', error => {
          console.error('ae', error);
        });
        eventEmitter.on('onVideoError', error => {
          console.error('ve', error);
        });
        eventEmitter.on('onMergeError', error => {
          console.error('me', error);
        });

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

        const uploadedVideo2S3 = await upload2S3(youtubeStream);
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
        eventEmitter.removeListener('onMergeError', args => console.log('remove listener', args));
      }
    }

    // CHECK VIDEOS
    console.log('videos', videos.length);
    console.log('uploadedVideosS3', Object.keys(uploadedVideosS3).length);

    // const checkLists = (listA, listB) => R.difference(listA, listB);

    // const pluckVideos = R.pluck('videoId')(R.pluck('contentDetails')(videos));
    // console.log('pluckVideos', pluckVideos)
    // const diff = checkLists(pluckVideos, Object.keys(uploadedVideosS3))
    const diff2 = checkLists(pluckVideos, Object.keys(uploadedVideosS3))
    console.log('Videos missing in S3:', diff2)
    diff.forEach(v => console.log(`https://www.youtube.com/watch?v=${v} - ${videos.find(video => video.contentDetails.videoId === v).snippet.title}`));
  })();

} catch (error) {
  console.log('Critical error: ******************************************************************************************');
  console.error(error);
}
const EventEmitter = require('events');
const eventEmitter = new EventEmitter();
const stream = require('stream');
const oneDriveAPI = require("onedrive-api");
const { S3, GetObjectCommand, HeadObjectCommand } = require('@aws-sdk/client-s3');
const { threadId } = require('worker_threads');
const fs = require('fs');
const axios = require('axios');
const dotenv = require('dotenv');

dotenv.config();

const s3 = new S3({ region: 'eu-west-1' });

// const bucket = 'streamvideotestbetisman';
const bucket = 'gs-youtube-migration';

class EmptyError extends Error {
  constructor(message) {
    super(message);
    this.name = "EmptyError";
    this.code = "EMPTY_VIDEO_LIST";
  }
}

const infoProgress = {
  currentVideo: '',
  download: {
    downloaded: 0,
    percent: 0,
  },
  upload: {
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

  return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
}

const printMessage = message => {
  process.stdout.clearLine();
  process.stdout.cursorTo(0);
  process.stdout.write(message);
}

eventEmitter.on('progressChanged', () => {
  printMessage(`${proxyProgress.currentVideo}\t|||\tdownloaded ${(proxyProgress.download.percent).toFixed(2)}%\tdownloaded bytes ${prettyBytes(proxyProgress.download.downloaded)}\t|||\tuploaded ${(proxyProgress.upload.percent).toFixed(2)}%\tuploaded bytes ${prettyBytes(proxyProgress.upload.uploaded)}`);
});

const stream2String = stream =>
  new Promise((resolve, reject) => {
    const chunks = [];
    stream.on("data", (chunk) => chunks.push(chunk));
    stream.on("error", reject);
    stream.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
  });

const getToken = async () => {
  const url = 'https://login.microsoftonline.com/common/oauth2/v2.0/token';
  console.log('------------------------------------------', process.env.AUTH_CODE)
  const auth_code = process.env.AUTH_CODE ?? 'ddd';
  const params = new URLSearchParams()
  params.append('redirect_uri', 'https://eo2s50w6m2oxn1q.m.pipedream.net')
  params.append('client_id', '123')
  params.append('scope', 'Files.ReadWrite.All')
  params.append('grant_type', 'authorization_code')
  params.append('code', auth_code)
  params.append('client_secret', '123')

  const config1 = {
    headers: {
      'Content-Type': 'application/x-www-form-urlencoded'
    }
  }
  const response1 = await axios.post(url, params, config1);
  console.log(response1.data.access_token)
  return response1.data;
};

(async () => {
  console.log("Beginning s3ToOneDrive");

  let BEARER_TOKEN;
  try {
    ({ access_token: BEARER_TOKEN } = await getToken());
    console.log('BEARER_TOKEN', BEARER_TOKEN)
  } catch (error) {
    console.log(error)
  }

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

  let uploadedVideosOneDrive;
  try {
    console.log('Downloading One Drive manifest');
    const items = await new Promise((resolve, reject) => oneDriveAPI.items
      .listChildren({
        accessToken: BEARER_TOKEN,
        // itemId: "015QRUT4SI5ZJFTQQSKNH2XX76GUXPVK2A",
        drive: "me", // 'me' | 'user' | 'drive' | 'group' | 'site'
        // driveId: "b!TuFAMdMsmEGmJwAdbQFKafFIce6sWtlDiAAIz7yM0eLoTwOH-wRjTrzDMrpF14SN", // BLANK | {user_id} | {drive_id} | {group_id} | {sharepoint_site_id}
      })
      .then((childrens) => {
        resolve(childrens)
      })
    );
    const folders = items.value.filter(x => x.name === 'KS Backups');
    if (!folders.length) throw new Error('Found more than 1 KS Backups folders');
    const [folder] = folders;
    const videos = await new Promise((resolve, reject) => oneDriveAPI.items
      .listChildren({
        accessToken: BEARER_TOKEN,
        itemId: folder.id,
        drive: "me", // 'me' | 'user' | 'drive' | 'group' | 'site'
        // driveId: "b!TuFAMdMsmEGmJwAdbQFKafFIce6sWtlDiAAIz7yM0eLoTwOH-wRjTrzDMrpF14SN", // BLANK | {user_id} | {drive_id} | {group_id} | {sharepoint_site_id}
      })
      .then((childrens) => {
        // console.log('childrens');
        resolve(childrens)
      }).catch(err => console.log(err))
    );
    console.log(videos, '....videos');
    if (!Object.keys(videos).length) throw new EmptyError('No videos found');
    const [manifest] = videos.value.filter(x => x.name === '_oneDrive_videos_uploaded.json');
    if (!manifest) throw new EmptyError('No manifest found');
    const manifestStream = oneDriveAPI.items.download({
      accessToken: BEARER_TOKEN,
      itemId: manifest.id,
    });
    const manifestString = await stream2String(manifestStream);
    console.log(manifestString, '........................manifestString');
    uploadedVideosOneDrive = JSON.parse(manifestString);
    console.log('Downloaded One Drive manifest');
  } catch (error) {
    console.error(error);
    if (error.code === 'EMPTY_VIDEO_LIST') {
      uploadedVideosOneDrive = {};
    }
    else throw error;
  }

  console.log('uploadedVideosS3', Object.keys(uploadedVideosS3).length);
  console.log('uploadedVideosOneDrive', Object.keys(uploadedVideosOneDrive).length);

  const isUploadedOneDrive = videoId => Object.entries(uploadedVideosOneDrive).filter(([key, value]) => key === videoId && value?.uploadedInfo?.key).length;

  let index = 0;
  for await (let [videoId, video] of Object.entries(uploadedVideosS3)) {
    // console.log(video)
    console.log('\n' + index + '\n');
    // if (index > 1) return;

    try {
      proxyProgress.currentVideo = videoId;

      console.log(`Uploading ${videoId} to One Drive`);

      const uploadVideo2Onedrive = async videoToUpload => {
        const {
          uploaded: {
            Key: videoKey,
          },
          uploadedInfo: {
            Key: jsonKey,
          }
        } = videoToUpload;

        if (isUploadedOneDrive(videoKey)) {
          console.log(`Skipping already uploaded ${videoKey}`);
          return;
        }

        console.log(`Heading ${videoKey} from S3`);

        const metadataCommand = new HeadObjectCommand({
          Bucket: bucket,
          Key: videoKey,
        });
        const metadata = await s3.send(metadataCommand);

        console.log(`Video ${videoKey} size from S3: ${metadata.ContentLength} bytes`);

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

        console.log(`Uploading ${videoKey} to One Drive streaming from S3`);

        const filename = videoKey;
        const fileSize = metadata.ContentLength;
        let pass = new stream.PassThrough();
        videoStream.pipe(pass)
        pass.on('finish', () => console.log('finish'))
        const uploadStart = Date.now();
        const uploaded = await new Promise(async (resolve, reject) => {
          try {
            await oneDriveAPI.items
              .uploadSession(
                {
                  accessToken: BEARER_TOKEN,
                  filename: filename,
                  fileSize: fileSize,
                  readableStream: pass,
                  // parentId: 'b!T',
                  parentPath: 'KS Backups',
                },
                (bytesUploaded) => {
                  proxyProgress.upload = {
                    uploaded: bytesUploaded,
                    percent: bytesUploaded / fileSize * 100,
                  }
                },
              )
              .then((item) => {
                console.log('item', 'uploaded ------------------------------------------------------------------');
                resolve(item);
              })
          } catch (error) {
            console.error(error);
            reject(error);
          }
        });
        const uploadFinish = Date.now();
        // console.log(uploaded, '......uploaded')
        console.log(`Uploaded ${videoKey} to One Drive streaming from S3 in ${uploadFinish - uploadStart} millis`);

        console.log(`Uploading video ${videoKey} info to One Drive ${jsonKey}`);

        const videoInfo = {
          oneDrive: uploaded,
          s3: video,
          metadata: {
            uploaded: Date.now(),
            uploadStart,
            uploadFinish,
            uploadTime: uploadFinish - uploadStart,
          },
        };
        const videoInfoStream = new stream.Readable.from([JSON.stringify(videoInfo || {}, null, 2)]);
        const uploadedSimple = await new Promise(async (resolve, reject) => {
          try {
            await oneDriveAPI.items
              .uploadSimple(
                {
                  accessToken: BEARER_TOKEN,
                  filename: jsonKey,
                  fileSize: JSON.stringify(videoInfo || {}, null, 2).length,
                  readableStream: videoInfoStream,
                  // parentId: 'b!TuFAMdMsmEGmJwAdbQFKafFIce6sWtlDiAAIz7yM0eLoTwOH-wRjTrzDMrpF14SN',
                  parentPath: 'KS Backups',
                },
                (bytesUploaded) => {
                  proxyProgress.upload = {
                    uploaded: bytesUploaded,
                    percent: bytesUploaded / fileSize * 100,
                  }
                },
              )
              .then((item) => {
                console.log('item', 'uploaded ------------------------------------------------------------------');
                resolve(item);
              })
          } catch (error) {
            console.error(error);
            reject(error);
          }
        });

        console.log(`Uploaded video ${videoKey} info to One Drive ${jsonKey} to ${uploadedSimple.webUrl}`);

        // Upload list of uploaded videos

        uploadedVideosOneDrive = {
          ...uploadedVideosOneDrive,
          [videoKey]: {
            ...uploadedVideosOneDrive[videoKey],
            uploaded,
            uploadedInfo: uploadedSimple,
            metadata: {
              uploaded: Date.now(),
            },
          },
        };
        const manifestUploadStream = new stream.Readable.from([JSON.stringify(uploadedVideosOneDrive || {}, null, 2)]);

        console.log('Uploading manifest info to One Drive _oneDrive_videos_uploaded.json');
        const uploadedSimpleManifest = await new Promise(async (resolve, reject) => {
          try {
            await oneDriveAPI.items
              .uploadSimple(
                {
                  accessToken: BEARER_TOKEN,
                  filename: '_oneDrive_videos_uploaded.json',
                  fileSize: JSON.stringify(uploadedVideosOneDrive || {}, null, 2).length,
                  readableStream: manifestUploadStream,
                  // parentId: 'b!TuFAMdMsmEGmJwAdbQFKafFIce6sWtlDiAAIz7yM0eLoTwOH-wRjTrzDMrpF14SN',
                  parentPath: 'KS Backups',
                },
                (bytesUploaded) => {
                  proxyProgress.upload = {
                    uploaded: bytesUploaded,
                    percent: bytesUploaded / fileSize * 100,
                  }
                },
              )
              .then((item) => {
                console.log('item', 'uploaded ------------------------------------------------------------------');
                resolve(item);
              })
          } catch (error) {
            console.error(error);
            reject(error);
          }
        });
        // console.log(uploadedSimpleManifest)
        console.log(`Uploaded manifest info to One Drive ${uploadedSimpleManifest.webUrl}`);

        // console.log(`Downloading manifest info from One Drive ${uploadedSimpleManifest.webUrl}`);
        // const manifestStream = oneDriveAPI.items.download({
        //   accessToken: BEARER_TOKEN,
        //   itemId: uploadedSimpleManifest.id,
        //   driveId: uploadedSimpleManifest.parentReference.driveId,
        // });
        // // fs.writeFileSync('_oneDrive_videos_uploaded.json', manifestStream, { enconding: 'utf-8' })
        // const writableStream = fs.createWriteStream('_oneDrive_videos_uploaded.json');
        // await new Promise((resolve, reject) => {
        //   manifestStream.pipe(writableStream);
        //   manifestStream.on('data', data => data)
        //   writableStream.on('end', () => resolve());
        //   writableStream.on('finish', () => resolve());
        //   writableStream.on('error', error => reject(error));
        //   manifestStream.on('error', error => reject(error));
        // });
        // console.log(`Downloaded manifest info from One Drive ${uploadedSimpleManifest.webUrl}`);
        fs.writeFileSync('_oneDrive_videos_uploaded.json', JSON.stringify(uploadedVideosOneDrive, null, 2), { econding: 'utf-8' })

      };

      await uploadVideo2Onedrive(video);


      index += 1;

    } catch (error) {
      console.log('-------------------------------------------------')
      console.error(error?.response?.data?.error || error);
      console.log('=================================================')
      // console.error(error?.response?.data?.error);
      // console.error(Object.keys(error?.response?.data));
      console.log('here')
    }
  }
})();
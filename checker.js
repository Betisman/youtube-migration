const axios = require('axios');
const { Readable, PassThrough } = require('stream');
let ffmpeg = require('fluent-ffmpeg');
const ffmpegPath = require('ffmpeg-static');
const fs = require('fs');
const R = require('ramda');
const dotenv = require('dotenv');

dotenv.config();

(async () => {
  const videos = JSON.parse(fs.readFileSync('all_the_videos.json', { encoding: 'utf-8' }));

  let index = 1;
  for await (let video of videos) {
    const {
      contentDetails: {
        videoId,
      },
      snippet: {
        title,
      },
    } = video;

    if (index === 306 || title === 'Private video') {
      index +=1;
      console.log(`${index} - ${videoId} is ${title}`)
      continue;
    }

    const config = {
      method: 'get',
      url: `${process.env.CLOUDFRONT_URL}/${videoId}`,
      responseType: 'stream',
    }

    const response = await axios(config)
    let progress = 0;
    response.data.on('data', chunk => {
      progress += chunk.length;
      // console.log(progress);
    })
    response.data.on('finish', data => console.log(progress));
    response.data.on('error', error => console.error(error));
    const passThroughStream = new PassThrough();
    response.data.pipe(passThroughStream);
    response.data.on('error', error => console.error(error));

    // create a new readable stream from whatever buffer you have
    // let readStream = new Readable()
    // readStream._read = () => { };
    // readStream.push(response.data)
    // readStream.push(null)

    const get_video_meta_data = async stream => new Promise((resolve, reject) => {
      ffmpeg.ffprobe(stream, (err, meta) => {
        if (err) {
          console.error('err', err);
          return reject(err);
        }
        // console.log('meta', meta)
        resolve(meta)
      })
    });
try {
    // I used a call to a promise based function to await the response
    let metadata = await get_video_meta_data(passThroughStream)
    // console.log('metadata', metadata)
    const { streams } = metadata;
    // if (streams.length !== 2) {
      console.log(`${index} - ${videoId} has ${streams.length} streams ${streams.length !== 2 ? "================================": ''} ... ${title} `)
    // }
    index += 1;
} catch (err) {
  console.error(`${index} - ${videoId} - ${title}`, err)
}
  }
})();
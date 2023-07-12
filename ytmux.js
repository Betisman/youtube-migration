/*/ //////////////////////////////////////////////////////////////////
\*\ ytdl-core-muxer: mostly copied from the ytdl-core example code :) 
\*\ credit and thanks for those authors: fent, TimeForANinja, coolaj86
/*/ //////////////////////////////////////////////////////////////////


// require all the things!
const ytdl = require('ytdl-core');
const ffmpegPath = require('ffmpeg-static');
const cp = require('child_process');
const stream = require('stream');

// default export: the ffmpeg muxer
const ytmux = ({ link, options = {}, eventEmitter }) => {
  const result = new stream.PassThrough({ highWaterMark: options.highWaterMark || 1024 * 512 });
  ytdl.getInfo(link, options?.video ?? {}).then(info => {
    audioStream = ytdl.downloadFromInfo(info, { quality: 'highestaudio', ...(options.audio || {}) });
    videoStream = ytdl.downloadFromInfo(info, { quality: 'highestvideo', ...(options.video || {}) });


    console.log(options.video)
    console.log({ quality: 'highestvideo', ...(options.video || {}) })

    if (eventEmitter) {
      // audioStream.removeListener('progress', args => console.log('remove listener onAudioError', args));
      audioStream.on('progress', (...args) => eventEmitter.emit('onAudioProgress', ...args));
      // videoStream.removeListener('progress', args => console.log('remove listener onAudioError', args));
      videoStream.on('progress', (...args) => eventEmitter.emit('onVideoProgress', ...args));
    }
    // create the ffmpeg process for muxing
    ffmpegProcess = cp.spawn(ffmpegPath, [
      // supress non-crucial messages
      '-loglevel', '8', '-hide_banner',
      // input audio and video by pipe
      '-i', 'pipe:3', '-i', 'pipe:4',
      // map audio and video correspondingly
      '-map', '0:a', '-map', '1:v',
      // no need to change the codec
      '-c', 'copy',
      // output mp4 and pipe
      '-f', 'matroska', 'pipe:5'
    ], {
      // no popup window for Windows users
      windowsHide: true,
      stdio: [
        // silence stdin/out, forward stderr,
        'inherit', 'inherit', 'inherit',
        // and pipe audio, video, output
        'pipe', 'pipe', 'pipe'
      ]
    });
    audioStream.pipe(ffmpegProcess.stdio[3]);
    videoStream.pipe(ffmpegProcess.stdio[4]);
    ffmpegProcess.stdio[5].pipe(result);

    audioStream.on('error', error => {
      console.error('audioStream error', error);
      const err = new Error(error);
      err.message = `Error on videoStream ${err.message}`;
      result.emit('error', err);
      audioStream.destroy();
    });
    videoStream.on('error', error => {
      console.error('videoStream error', error);
      const err = new Error(error);
      err.message = `Error on videoStream ${err.message}`;
      result.emit('error', err);
      videoStream.destroy();
    });
    ffmpegProcess.stdio[5].on('error', error => {
      console.error('ffmpegProcess.stdio[5] error', error);
     const err = new Error(error);
      err.message = `Error on videoStream ${err.message}`;
      result.emit('error', err);
      ffmpegProcess.stdio[5].destroy();
    });

    result.on('data', data => eventEmitter.emit('onMergeProgress', data.length));
    result.on('error', error => {
      eventEmitter.emit('onMergeError', new Error('Error on result', { cause: error }));
      result.destroy();
      // throw new Error('Error on result', { cause: error });
    });
  })
  .catch(error => {
    console.error('catch', error);
    result.emit('error', error);
    // throw error;
  });
  return result;
};

// export it
module.exports = ytmux;

// export other functions, in case you want them
ytmux.download = ytdl;
ytmux.chooseFormat = ytdl.chooseFormat;
ytmux.downloadFromInfo = ytdl.downloadFromInfo;
ytmux.filterFormats = ytdl.filterFormats;
ytmux.getBasicInfo = ytdl.getBasicInfo;
ytmux.getInfo = ytdl.getInfo;
ytmux.getURLVideoID = ytdl.getURLVideoID;
ytmux.getVideoID = ytdl.getVideoID;
ytmux.validateID = ytdl.validateID;
ytmux.validateURL = ytdl.validateURL;

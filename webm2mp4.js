const ffmpegPath = require('ffmpeg-static');
const cp = require('child_process');
const stream = require('stream');

// default export: the ffmpeg muxer
const webm2mp4 = ({ videoStream, options = {}, eventEmitter }) => {
  const result = new stream.PassThrough({ highWaterMark: options.highWaterMark || 1024 * 512 });

  if (eventEmitter) {
    videoStream.on('progress', (...args) => eventEmitter.emit('onVideoProgress', ...args));
    videoStream.on('data', (...args) => eventEmitter.emit('onVideoData', ...args));
  }
  // create the ffmpeg process for muxing
  ffmpegProcess = cp.spawn(ffmpegPath, [
    '-i', 'pipe:3',
  ], {
    // no popup window for Windows users
    windowsHide: true,
    stdio: [
      // silence stdin/out, forward stderr,
      'inherit', 'inherit', 'inherit',
      // and pipe video, output
      'pipe', 'pipe'
    ]
  });
  videoStream.pipe(ffmpegProcess.stdio[3]);
  ffmpegProcess.stdio[4].pipe(result);

  videoStream.on('error', error => {
    console.error('videoStream error', error);
    const err = new Error(error);
    err.message = `Error on videoStream ${err.message}`;
    result.emit('error', err);
    videoStream.destroy();
  });
  ffmpegProcess.stdio[4].on('error', error => {
    console.error('ffmpegProcess.stdio[4] error', error);
    const err = new Error(error);
    err.message = `Error on videoStream ${err.message}`;
    result.emit('error', err);
    ffmpegProcess.stdio[5].destroy();
  });

  result.on('data', data => eventEmitter.emit('onResultData', data.length));
  result.on('error', error => {
    eventEmitter.emit('onMergeError', new Error('Error on result', { cause: error }));
    result.destroy();
    // throw new Error('Error on result', { cause: error });
  });
  return result;
};

// export it
module.exports = webm2mp4;

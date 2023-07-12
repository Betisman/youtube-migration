(async () => {
  const { google } = require('googleapis');
  const fs = require('fs');
  const dotenv = require('dotenv');

  dotenv.config();

  console.log("Beginning youtubeIndexer. Checking for valid oauth.");

  const yt_refresh_token = process.env.YOUTUBE_REFRESH_TOKEN;
  const yt_client_id = process.env.YOUTUBE_CLIENT_ID;
  const yt_client_secret = process.env.YOUTUBE_CLIENT_SECRET;
  const yt_channel_id = process.env.YOUTUBE_CHANNEL_ID;

  const oauthClient = new google.auth.OAuth2({
    clientId: yt_client_id,
    clientSecret: yt_client_secret,
    redirectUri: 'http://localhost'
  });

  oauthClient.setCredentials({
    refresh_token: yt_refresh_token
  });

  const youtube = google.youtube("v3");
  // const channelResult = await youtube.channels.list({
  //   auth: oauthClient,
  //   part: ['snippet', 'contentDetails'],
  //   id: [yt_channel_id]
  // });
  // console.log('1', yt_channel_id, channelResult.data.items[0].contentDetails.relatedPlaylists.uploads)
let nextPageToken = undefined;
let videosFetched = 0;

let page = 0;
fs.writeFileSync('all_the_videos.json', '', { encoding: 'utf-8' });

do {
  const videosResult = await youtube.playlistItems.list({
    auth: oauthClient,
    maxResults: 50,
    pageToken: nextPageToken,
    part: ['snippet', 'status', 'id', 'contentDetails'],
    playlistId: process.env.YOUTUBE_PLAYLIST,
    // playlistId: channelResult.data.items[0].contentDetails.relatedPlaylists.uploads,
  });
  // console.log(videosResult.data.items)
  videosFetched += videosResult.data.items.length;
  page += 1;
  
  nextPageToken = videosResult.data.nextPageToken;
  
  videosResult.data.items.map((video, index) => {
    fs.appendFileSync('all_the_videos.json', `${index === 0 && page === 1  ? '[' : ','}${JSON.stringify(video, null, 2)}`, { encoding: 'utf-8', flag: 'a' });
  });
} while (nextPageToken);

  fs.appendFileSync('all_the_videos.json', ']', { encoding: 'utf-8', flag: 'a' });
// console.log(channelResult.data.items)
console.log(`${videosFetched} videos fetched`)
})();

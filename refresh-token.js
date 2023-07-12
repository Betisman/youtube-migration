(async () => {
  const { google } = require('googleapis');
  const prompts = require('prompts');
  const dotenv = require('dotenv');

  dotenv.config();

  console.log("about to execute oauth");

  const yt_client_id = process.env.YOUTUBE_CLIENT_ID;
  const yt_client_secret = process.env.YOUTUBE_CLIENT_SECRET;

  const oauthClient = new google.auth.OAuth2({
    clientId: yt_client_id,
    clientSecret: yt_client_secret,
    redirectUri: 'http://localhost'
  });

  const authUrl = oauthClient.generateAuthUrl({
    access_type: 'offline', //gives you the refresh_token
    scope: 'https://www.googleapis.com/auth/youtube.readonly'
  });

  const codeUrl = await prompts({
    type: 'text',
    name: 'codeURl',
    message: `Please go to \n\n${authUrl}\n\nand paste in resulting localhost uri`
  });

  const decodedUrl = decodeURIComponent(codeUrl.codeURl);
  const code = decodedUrl.split('?code=')[1].split("&scope=")[0];
  const token = (await oauthClient.getToken(code)).tokens;
  const yt_refresh_token = token.refresh_token;
  console.log(`Please save this value into the YOUTUBE_REFRESH_TOKEN env variable for future runs: ${yt_refresh_token}`);

  await prompts({
    type: 'text',
    name: 'blank',
    message: 'Hit enter to exit:'
  });

  process.exit(0);
})();
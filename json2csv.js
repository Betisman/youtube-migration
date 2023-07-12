const { join } = require('path');
const { stderr } = require('process');

(async () => {
  try {

    const fs = require('fs');

    const DELIMITER = ';';
    const INCLUDE_HEADER = true;

    console.log("Beginning json2csv. Checking for valid oauth.");

    const videos = JSON.parse(fs.readFileSync('all_the_videos.json', { encoding: 'utf-8' }));

    console.log(videos.length);

    fs.writeFileSync('all_the_videos.csv', '', { encoding: 'utf-8' })

    const trim = str => str?.trim();
    const escapeNewLines = str => str?.replace(/\n/g, '\\n');
    const checkRow = (data, row) => {
      const numRows = Object.keys(data).length;
      const columns = row.split(DELIMITER).length;
      return numRows === columns;
    };

    videos.forEach((video, index) => {
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
      } = video;

      const data = {
        id: videoId,
        publishedAt: videoPublishedAt,
        privacyStatus,
        title,
        description,
        url: `https://www.youtube.com/watch?v=${videoId}`,
      };

      if (INCLUDE_HEADER && !index) {
        const header = Object.keys(data).join(DELIMITER);
        fs.appendFileSync('all_the_videos.csv', `${header}\n`, { encoding: 'utf-8', flag: 'a' });
      }

      const row = Object.values(data).map(trim).map(escapeNewLines).join(DELIMITER);
      if (!checkRow(data, row)) {
        throw new Error(`ERROR at ${row}`);
      }

      fs.appendFileSync('all_the_videos.csv', `${row}\n`, { encoding: 'utf-8', flag: 'a' });
    });
  } catch (error) {
    console.error(error);
  }
}) ();
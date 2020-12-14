const axios = require("axios");
const fetch = require('node-fetch');

function attempt(promise) {
  return promise
    .then(data => [data, null])
    .catch(error => Promise.resolve([null, error]));
}

class TweetFetcher {
  constructor() {
    return;
  }
  
  async sleep(ms) {
    return new Promise((accept, reject) => {
      setTimeout(accept, ms);
    });
  }

  async go(uid, config) {
    let uidIndex = config.params.indexOf("userId=");
    let ampIndex = config.params.indexOf("&", uidIndex);
    let params = config.params.substr(0, uidIndex) + "userId=" + uid + config.params.substr(ampIndex);

    let url = `https://api.twitter.com/2/timeline/profile/${uid}.json?${params}`;
    let resp = await axios.get(url, {headers: config.headers})
    return resp;
  }
}

exports.handler = async (event, context) => {
  console.log("IN HANDLER");
  let fetcher = new TweetFetcher();
  let config = event.task.job.config;

  console.log("MAKING FETCH ATTEMPT");
  let [twitterResp, err] = await attempt(fetch("https://api.twitter.com/2/timeline/profile/25073877.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=false&count=20&userId=25073877&ext=mediaStats%2ChighlightedLabel", {
    "credentials": "include",
    "headers": {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0",
        "Accept": "*/*",
        "Accept-Language": "en-US,en;q=0.5",
        "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
        "x-guest-token": "1334357759635156994",
        "x-twitter-client-language": "en",
        "x-twitter-active-user": "yes",
        "x-csrf-token": "378144aa5c34f5090bceec8b3186fb19",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache"
    },
    "referrer": "https://twitter.com/",
    "method": "GET",
    "mode": "cors"
  }));
  let j = await twitterResp.json();
  console.log(j);

  let resp;
  if(err) {
    resp = {
      statusCode: 400,
      body: err
    }
  }
  else {
    resp = {
      statusCode: 200,
      tweets: j/*.globalObjects.tweets*/
    }
  }
  return resp;
};

// (async () => {
//   //let resp = await exports.handler({"task": {"job": {"handle": "potus44", "uid": "1536791610"}}});
//   let resp = await exports.handler({"task": {"job": {"handle": "realDonaldTrump", "uid": "25073877"}}});
//   console.log(JSON.stringify(resp));
// })();

// (async () => {
//   let res = await fetch("https://api.twitter.com/2/timeline/profile/25073877.json?include_profile_interstitial_type=1&include_blocking=1&include_blocked_by=1&include_followed_by=1&include_want_retweets=1&include_mute_edge=1&include_can_dm=1&include_can_media_tag=1&skip_status=1&cards_platform=Web-12&include_cards=1&include_ext_alt_text=true&include_quote_count=true&include_reply_count=1&tweet_mode=extended&include_entities=true&include_user_entities=true&include_ext_media_color=true&include_ext_media_availability=true&send_error_codes=true&simple_quoted_tweet=true&include_tweet_replies=false&count=20&userId=25073877&ext=mediaStats%2ChighlightedLabel", {
//     "credentials": "include",
//     "headers": {
//         "User-Agent": "Mozilla/5.0 (X11; Linux x86_64; rv:68.0) Gecko/20100101 Firefox/68.0",
//         "Accept": "*/*",
//         "Accept-Language": "en-US,en;q=0.5",
//         "authorization": "Bearer AAAAAAAAAAAAAAAAAAAAANRILgAAAAAAnNwIzUejRCOuH5E6I8xnZz4puTs%3D1Zv7ttfk8LF81IUq16cHjhLTvJu4FA33AGWWjCpTnA",
//         "x-guest-token": "1334357759635156994",
//         "x-twitter-client-language": "en",
//         "x-twitter-active-user": "yes",
//         "x-csrf-token": "378144aa5c34f5090bceec8b3186fb19",
//         "Pragma": "no-cache",
//         "Cache-Control": "no-cache"
//     },
//     "referrer": "https://twitter.com/",
//     "method": "GET",
//     "mode": "cors"
//   });
//   let j = await res.json();
//   console.log(j);
// })();
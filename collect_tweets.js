const lineByLine = require('n-readlines');
var AWS = require('aws-sdk');
AWS.config.update({region: "us-west-1"});
const asyncRedis = require("async-redis");
const fs = require("fs");

class TweetFetcher {
    constructor(lambdaCount, lambdaBaseName, outputDir) {
        //let lambdaNum = parseInt(Math.random() * 100);
        this.lambdaBaseName = lambdaBaseName;
        this.lambdaCount = lambdaCount;
        this.client = asyncRedis.createClient();
        this.outputDir = outputDir;
        //this.getTweets(`twint_gamma_${1}`, "potus44", 20);

        this.usernames = [];
        this.limit = 100;
        this.totals = {};
    }

    async mainLoop() {
        let intv = setInterval(async () => {
            let newLength = await this.client.llen("id_to_username");
            //console.log("The new length of the usernames list is " + newLength);
            if(newLength > this.usernames.length) {
                console.log("Okay, adding these usernames");
                let newItemsCount = newLength - this.usernames.length;
                let newItems = await this.client.lrange("id_to_username", 0, newItemsCount - 1);
                console.log(newItems);
                let newUsernames = newItems.map(x => x.split("\t")[1]);
                //console.log(newUsernames);
                this.usernames = this.usernames.concat(newUsernames);

                for(let i = 0; i < newUsernames.length; i++) {
                    let latestTimestamp = await this.client.get(`timestamps_${newUsernames[i]}`);
                    if(latestTimestamp) {
                        console.log(`${newUsernames[i]} is already in the database.`);
                    }
                    else {
                        let payload = {
                            "Username": newUsernames[i],
                            "Limit": this.limit,
                            "Store_object": true
                        };
                        this.client.lpush("payloads", JSON.stringify(payload));
                    }
                }
            }

            clearInterval(intv); // we only wanna do it once since we got all the names already
        }, 1000); // once per second, check for new usernames
        
        // let un = [];
        // for(let i = 0; i < 5; i++) {
        //     un.push(this.usernames[i]);
        // }
        // this.usernames = un;

        // Restore pending requests
        let pending = await this.client.smembers("pending");
        for(let i = 0; i < pending.length; i++) {
            console.log("Restoring pending payload " + pending[i]);
            let payload = await this.client.get(`pending_${pending[i]}`);
            this.client.lpush("payloads", payload);
        }
        
        this.client.del("pending");
        for(let i = 0; i < pending.length; i++) {
            this.client.del(`pending_${pending[i]}`);
        }

        let lambdaIndex = 0;
        setInterval(async () => {
            let payload = await this.client.lpop("payloads");
            if(payload) {
                payload = JSON.parse(payload);
                this.backup(payload);

                let lambdaName = this.lambdaBaseName + lambdaIndex;
                if(payload) {
                    this.getTweets(lambdaName, payload.Username, payload.Limit, payload.Resume);
                }
                lambdaIndex = ++lambdaIndex % this.lambdaCount;
            }
        }, 50);
    }

    backup(payload) {
        //console.log("BACKING UP PAYLOAD " + typeof(payload));
        //console.log(payload);
        let key = `pending_${payload.Username}`;
        //console.log(payload.Username + "is pending");

        this.client.sadd("pending", key); // note this is pending with some uid that we store here, and set in the big database
        this.client.set(key, JSON.stringify(payload));
    }

    unbackup(username) {
        let key = `pending_${username}`;
        this.client.srem("pending", key);
        this.client.del(key);
    }

    getTweets(lambdaName, username, limit, resume) {
        //console.log("FETCHING TWEETS THROUGH " + lambdaName + " with limit " + limit);
        let payload = {
            "Username": username,
            "Limit": limit,
            "Store_object": true
        };

        if(resume) {
            payload["Resume"] = resume;
        }
        
        //console.log(payload);

        var params = {
            FunctionName: lambdaName,
            Payload: JSON.stringify(payload)
        };

        var lambda = new AWS.Lambda();
        
        var d = new Date();
        var n = d.getTime();

        lambda.invoke(params, (err, data) => {
            if(err) {
                d = new Date();
                console.log("There was an error for " + username + " through " + lambdaName + " after " + ((d.getTime() - n)));
                console.log(err);
                this.client.lpush("payloads", JSON.stringify(payload));
            }
            else {
                d = new Date();
                console.log("Victory for " + username + " in " + (d.getTime() - n) + " with " + lambdaName + ".");
                let res = data["Payload"];
                res = JSON.parse(res);
                
                let tweets = res["tweets_list"];
                if(tweets.length > 0) {
                    if(!(username in this.totals)) {
                        this.totals[username] = 0;
                    }
                    this.totals[username] += tweets.length;

                    console.log("Fetched " + tweets.length + " tweets for " + username + ", bringing their total to " + this.totals[username]);
                    //console.log("Here's the first one: ");
                    //console.log(tweets[0]);

                    //console.log("\n");
                    let timestamp = tweets[tweets.length - 1].datestamp + " " + tweets[tweets.length - 1].timestamp;

                    let ofname = `${this.outputDir}/${username}.txt`;
                    var stream = fs.createWriteStream(ofname, {flags:'a'});                
                    for(let i = 0; i < tweets.length; i++) {
                        stream.write(`${JSON.stringify(tweets[i])}\n`);
                    }
                    stream.end();

                    payload["Resume"] = res["Resume"]; //timestamp;
                    this.client.lpush("payloads", JSON.stringify(payload));
                    this.client.set(`timestamps_${username}`, timestamp);
                }
                else {
                    console.log("We're done with " + username);
                }
            }

            this.unbackup(username);
        });
    }
}

(async () => {
    let t = new TweetFetcher(1000, "twint_gamma_", "../big_drive/results");
    await t.mainLoop();
})();
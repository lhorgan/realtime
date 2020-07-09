const lineByLine = require('n-readlines');
var AWS = require('aws-sdk');
AWS.config.update({region: "us-west-1"});
const asyncRedis = require("async-redis");
const fs = require("fs");
const fetch = require('node-fetch');

function randomString() {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

/*******
 * 
 * 
 * 
 * 
 */
class Lambda {
    constructor() {
        this.servers = ["http://127.0.0.1:8081"];

        this.pending = [];
        this.interval = 5000;
        this.sendLength = 2;
        this.process();
    }

    invoke(params, cb) {
        this.pending.push([params, cb]);
        //console.log("ADDING " + params + " to pending");
    }

    handleErrors(response) {
        if(!response.ok) {
            throw Error(response.statusText);
        }
        return response;
    }

    process() {
        setInterval(() => {
            let toSend = {"payloads": []};
            let cbDict = {};

            if(this.pending.length > this.sendLength) {
                for(let i = 0; i < this.sendLength; i++) {
                    let id = randomString();
                    let [params, cb] = this.pending[i];
                    toSend.payloads.push([id, params]);
                    cbDict[id] = cb;
                }
                this.pending = this.pending.slice(this.sendLength, this.pending.length);
            }

            let server = this.servers[0];
            fetch(server + "/payloads", {
                method: "post",
                body: JSON.stringify(toSend),
                headers: { 'Content-Type': 'application/json' }
            })
            .then(this.handleErrors)
            .then(response => response.json())
            .then(resp => {
                for(let i = 0; i < resp.length; i++) {
                    let [reqID, err, data] = resp[i];
                    cbDict[reqID](err, data);
                }
            })
            .catch(err => {
                console.log(err);
                console.log("Something seems to have gone wrong...");
                for(let reqID in cbDict) {
                    cbDict[reqID]("REQUEST ERROR ON OUR END", null);
                }
            });            
        }, this.interval);
    }
}

class TweetFetcher {
    constructor(lambdaCount, lambdaBaseName, outputDir) {
        //let lambdaNum = parseInt(Math.random() * 100);
        this.lambdaBaseName = lambdaBaseName;
        this.idling = [];
        this.lambdaCount = lambdaCount;
        this.client = asyncRedis.createClient();
        this.outputDir = outputDir;
        //this.getTweets(`twint_gamma_${1}`, "potus44", 20);

        this.usernames = [];
        this.limit = 100;
        this.totals = {};

        this.lbda = new Lambda();
    }

    async mainLoop() {
        let newLength = await this.client.llen("id_to_username");
        let newItemsCount = newLength - this.usernames.length;
        let newItems = await this.client.lrange("id_to_username", 0, newItemsCount - 1);
        let newUsernames = newItems.map(x => x.split("\t")[1]);
        this.usernames = this.usernames.concat(newUsernames);

        for(let i = 0; i < newUsernames.length; i++) {
            let latestTimestamp = await this.client.get(`timestamps_${newUsernames[i]}`);
            if(latestTimestamp) {
                //console.log(`${newUsernames[i]} is already in the database.`);
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

        // Restore pending requests
        // let pending = await this.client.smembers("pending");
        // for(let i = 0; i < pending.length; i++) {
        //     //console.log("Restoring pending payload " + pending[i]);
        //     let payload = await this.client.get(`pending_${pending[i]}`);
        //     this.client.lpush("payloads", payload);
        // }
        
        // this.client.del("pending");
        // for(let i = 0; i < pending.length; i++) {
        //     this.client.del(`pending_${pending[i]}`);
        // }

        for(let lambdaIndex = 0; lambdaIndex < this.lambdaCount; lambdaIndex++) {
            let payload = await this.client.lpop("payloads");
            payload = JSON.parse(payload);
            this.backup(payload);

            let lambdaName = this.lambdaBaseName + lambdaIndex;
            if(payload) {
                this.getTweets(lambdaName, payload.Username, payload.Limit, payload.Resume);
            }
            await this.wait(25);
        }

        this.slothIsASin();
    }
    
    wait(ms) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve();
            }, ms)
        });
    }

    slothIsASin() {
        // may no Labmda rest, EVER!  Unless it's Sunday.
        setInterval(async () => {
            if(this.idling.length > 0) {
                let lambdaName = this.idling.shift();
                //console.log(lambdaName + " is idle.");
                let payload = await this.client.lpop("payloads");
                if(payload) {
                    payload = JSON.parse(payload);
                    this.backup(payload);
                    if(payload) {
                        this.getTweets(lambdaName, payload.Username, payload.Limit, payload.Resume);
                    }
                }
                else {
                    this.idling.push(lambdaName);
                }
            }
        }, 25);
    }

    backup(payload) {
        let key = `pending_${payload.Username}`;

        this.client.sadd("pending", key); // note this is pending with some uid that we store here, and set in the big database
        this.client.set(key, JSON.stringify(payload));
    }

    unbackup(username) {
        let key = `pending_${username}`;
        this.client.srem("pending", key);
        this.client.del(key);
    }

    getTweets(lambdaName, username, limit, resume) {
        let payload = {
            "Username": username,
            "Limit": limit,
            "Store_object": true
        };

        if(resume) {
            payload["Resume"] = resume;
        }

        var params = {
            FunctionName: lambdaName,
            Payload: JSON.stringify(payload)
        };
        
        var d = new Date();
        var n = d.getTime();

        this.lbda.invoke(params, (err, data) => {
            this.idling.push(lambdaName); // this lambda is now available

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
async function sleep(ms) {
    return new Promise((accept, reject) => {
        setTimeout(accept, ms);
    });
}

const lineByLine = require('n-readlines');
var AWS = require('aws-sdk');
const express = require("express");
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
                //console.log(err);
                //console.log("Something seems to have gone wrong...");
                for(let reqID in cbDict) {
                    cbDict[reqID]("REQUEST ERROR ON OUR END", null);
                }
            });            
        }, this.interval);
    }
}

class TweetFetcher {
    constructor(lambdaCount, lambdaBaseName, inputFile, outputDir) {
        //let lambdaNum = parseInt(Math.random() * 100);
        this.lambdaBaseName = lambdaBaseName;
        this.idling = [];
        this.lambdaCount = lambdaCount;
        this.client = asyncRedis.createClient();
        this.outputDir = outputDir;
        this.inputFile = inputFile;
        //this.getTweets(`twint_gamma_${1}`, "potus44", 20);

        this.usernames = [];
        this.limit = 100;
        this.totals = {};

        this.app = express();
        this.app.use(express.json());
        this.server = require('http').createServer(this.app);
        this.port = 3050;

        this.headersList = [];
        this.headersDict = {};
        this.headersCount = 500; // we'll go for 500 right now
        this.headersIndex = 0;

        this.prevHandles = {};

        //this.lbda = new Lambda(); // CRITICAL PUT THIS BACK!
    }

    async mainLoop() {
        let readstream = new lineByLine(this.inputFile);
        let line = readstream.next();
        let ctr = 0;
        while((line = readstream.next()) && ctr++ < 300) {
            //console.log(line.toString());
            //console.log(line.toString());
            let comps =  line.toString().split("\t");
            let payload = {
                "uid": comps[0],
                "handle": comps[2] 
            }
            //console.log(payload);
            await this.client.lpush("payloads", JSON.stringify(payload));
        }

        // for(let lambdaIndex = 0; lambdaIndex < this.lambdaCount; lambdaIndex++) {
        //     let payload = await this.client.lpop("payloads");
        //     payload = JSON.parse(payload);
        //     this.backup(payload);

        //     let lambdaName = this.lambdaBaseName + lambdaIndex;
        //     if(payload) {
        //         this.getTweets(lambdaName, payload.Username, payload.Limit, payload.Resume);
        //     }
        //     await this.wait(25);
        // }

        this.slothIsASin();
    }

    listenHTTP() {
        //console.log("SERVER LISTENING ON " + this.port);
        // Listen on the port specified in console args
        this.server.listen(this.port, ()  => {});

        let firstGo = true;
        let ctr = 0;
        /**
         */
        this.app.post("/credentials", async (req, res) => {
            //console.log(req.body);

            let headers = req.body.headers;
            let url = req.body.url;
            
            //console.log(headers);

            if("x-csrf-token" in headers) {
                //console.log("WE HAVE SOME HEADERS! " + headers["x-csrf-token"]);;
                if(!(headers["x-csrf-token"] in this.headersDict)) {
                    //console.log("AND THEY ARE NEW");
                    this.headersDict[headers["x-csrf-token"]] = {headers, url};
                    this.headersList[this.headersIndex] = {headers, url};
                    let lambdaName = this.lambdaBaseName + this.headersIndex;
                    this.headersIndex = (this.headersIndex + 1) % this.headersCount;
                    
                    
                    if(++ctr >= this.headersCount) {
                        firstGo = false;
                    }

                    if(firstGo === true) {
                        //console.log("setting " + lambdaName + " to idling...");
                        this.idling.push(lambdaName); // this server is ready to go!
                    }
                }
            }

            res.send({"status": 200});
        });
    }    
    
    wait(ms) {
        return new Promise((resolve, reject) => {
            setTimeout(() => {
                resolve();
            }, ms)
        });
    }

    async slothIsASin() {
        // may no Labmda rest, EVER!  Unless it's Sunday.
        // setInterval(async () => {
        //     if(this.idling.length > 0) {
        //         let lambdaName = this.idling.shift();
        //         //console.log(lambdaName + " is idle.");
        //         let payload = await this.client.lpop("payloads");
        //         if(payload) {
        //             payload = JSON.parse(payload);
        //             this.backup(payload);
        //             if(payload) {
        //                 this.getTweets(lambdaName, payload.Username, payload.Limit, payload.Resume);
        //             }
        //         }
        //         else {
        //             this.idling.push(lambdaName);
        //         }
        //     }
        // }, 25);
        while(true) {
            if(this.idling.length > 0) {
                let lambdaName = this.idling.shift();
                let lambdaNum = lambdaName.split("_");
                lambdaNum = parseInt(lambdaNum[lambdaNum.length - 1]);
                let payload = await this.client.lpop("payloads");
                if(payload) {
                    payload = JSON.parse(payload);
                    let headers = this.headersList[lambdaNum];
                    //console.log("Here is our payload:");
                    //console.log(payload);
                    //console.log("Here are our headers");
                    //console.log(headers);
                    //console.log("\n\n");

                    this.getTweets(payload, headers, lambdaName);
                }
                else {
                    this.idling.push(lambdaName);
                }
            }
            await sleep(100);
        }
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

    getTweets(payload, config, lambdaName) {
        payload.config = config;
        let prevHandle = this.prevHandles[lambdaName];
        let username = payload.username; // same as handle
        if(prevHandle) {
            payload.config.headers.referer = `https://twitter.com/${prevHandle}`;
        }
        this.prevHandles[lambdaName] = username; 

        //console.log(payload);

        let finalPayload = {
            "uid": payload.uid,
            "url": payload.config.url,
            "headers": payload.config.headers,
            "handle": payload.handle
        }
        console.log(JSON.stringify(finalPayload));

        var params = {
            FunctionName: lambdaName,
            Payload: JSON.stringify(finalPayload)
        };
        
        var d = new Date();
        var n = d.getTime();
        return;
        this.lbda.invoke(params, (err, data) => {
            this.idling.push(lambdaName); // this lambda is now available

            if(err) {
                d = new Date();
                //console.log("There was an error for " + username + " through " + lambdaName + " after " + ((d.getTime() - n)));
                //console.log(err);
                this.client.lpush("payloads", JSON.stringify(payload));
            }
            else {
                d = new Date();
                //console.log("Victory for " + username + " in " + (d.getTime() - n) + " with " + lambdaName + ".");
                let res = data["Payload"];
                res = JSON.parse(res);
            }

            this.unbackup(username);
        });
    }
}

(async () => {
    let t = new TweetFetcher(1000, "twint_gamma_", "/home/luke/Documents/lazer/achtung/id_handle_mapping.tsv", "./results");
    await t.mainLoop();
    t.listenHTTP();
})();
const lineByLine = require('n-readlines');
const request = require("request");
var AWS = require('aws-sdk');
AWS.config.update({region: "us-west-1"});
const asyncRedis = require("async-redis");

class TweetFetcher {
    constructor(lambdaCount, lambdaBaseName) {
        //let lambdaNum = parseInt(Math.random() * 100);
        this.lambdaBaseName = lambdaBaseName;
        this.lambdaCount = lambdaCount;
        this.client = asyncRedis.createClient();
        //this.getTweets(`twint_gamma_${1}`, "potus44", 20);

        this.usernames = [];
        this.limit = 100;
    }

    mainLoop() {
        setInterval(async () => {
            let newLength = await this.client.llen("id_to_username");
            console.log("The new length of the usernames list is " + newLength);
            if(newLength > this.usernames.length) {
                console.log("Okay, adding these usernames");
                let newItemsCount = newLength - this.usernames.length;
                let newItems = await this.client.lrange("id_to_username", 0, newItemsCount - 1);
                console.log(newItems);
                let newUsernames = newItems.map(x => x.split("\t")[1]);
                //console.log(newUsernames);
                this.usernames = this.usernames.concat(newUsernames);

                for(let i = 0; i < newUsernames.length; i++) {
                    let payload = {
                        "Username": newUsernames[i],
                        "Limit": this.limit,
                        "Store_object": true
                    };
                    this.client.lpush("payloads", JSON.stringify(payload));
                }
            }
        }, 1000); // once per second, check for new usernames

        let lambdaIndex = 0;
        setInterval(async () => {
            let payload = await this.client.lpop("payloads");
            payload = JSON.parse(payload);
            if(payload) {
                let lambdaName = this.lambdaBaseName + lambdaIndex;
                if(payload) {
                    this.getTweets(lambdaName, payload.Username, payload.Limit, payload.Until);
                }
                lambdaIndex = ++lambdaIndex % this.lambdaCount;
            }
        }, 5000);
    }

    getTweets(lambdaName, username, limit, until) {
        console.log("FETCHING TWEETS THROUGH " + lambdaName);
        let payload = {
            "Username": username,
            "Limit": limit,
            "Store_object": true
        }

        if(until) {
            payload["Until"] = until;
        }
        
        console.log(payload);

        var params = {
            FunctionName: lambdaName,
            Payload: JSON.stringify(payload)
        };

        var lambda = new AWS.Lambda();
        lambda.invoke(params, (err, data) => {
            let lambdaResp = {};

            if(err) {
                console.log("There was an error");
                lambdaResp["error"] = true;
            }
            else {
                console.log("we successed");
                let res = data["Payload"];
                res = JSON.parse(res);
                //console.log(res);
                for(let i = 0; i < res.length; i++) {
                    let timestamp = res[i].datestamp + " " + res[i].timestamp;
                    //console.log(timestamp);
                    //console.log(res[i].id_str);
                }
                //cb(res);
            }
        });
    }
}

let t = new TweetFetcher(500, "twint_gamma_");
t.mainLoop();

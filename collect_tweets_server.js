var express = require('express');
var app = express();
var bodyParser = require('body-parser')
var AWS = require('aws-sdk');
AWS.config.update({region: "us-west-1"});

app.use(bodyParser.json({limit: '50mb'}));

class Earl {
    constructor() {
        this.configure();
    }

    configure() {
        app.listen(8081, function () {
            console.log("App listening on port 8081");
        });

        app.post("/payloads", (req, res) => {
            console.log("payloads received");
            console.log(req.body);
            let toSend = req.body.payloads;
            this.collectTweets(toSend, (response) => {
                console.log("SENDING A RESPONSE");
                res.send(JSON.stringify(response));
            });
        });
    }
    
    collectTweets(toSend, cb) {
        let response = [];
        for(let i = 0; i < toSend.length; i++) {
            let [id, params] = toSend[i];
            this.collectHelper(id, params, (reqID, err, data) => {
                response.push([reqID, err, data]);
                console.log("RESP LENGTH IS NOW " + response.length + " of " + toSend.length);
                if(response.length === toSend.length) {
                    cb(response);
                }
            });
        }
    }

    collectHelper(reqID, params, cb) {
        var lambda = new AWS.Lambda();
        lambda.invoke(params, (err, data) => {
            if(err) {
                console.log("HERE IS OUR ERROR");
                console.log(err);
                cb(reqID, err, null);
            }
            else {
                //console.log("HERE IS OUR DATA");
                //console.log(data);
                cb(reqID, null, data);
            }
        });
    }
}

let e = new Earl();